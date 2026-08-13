using System.Text;
using ChatApp.Realtime.IntegrationTests.Measurement;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// P0：OUTBOX-DB-1（消息与 Outbox 数据库瘦身）的 PG 级测量基石。
/// <para>
/// 以 <c>pg_stat_statements</c> + <c>pg_stat_wal</c> 采集真实 SaveAsync 热路径的
/// 窗口增量，把每消息成本拆成 message、conversation/unread、outbox insert 的可归因
/// SQL/WAL 项，并追加 outbox claim + complete 的排水成本。报告写入
/// <c>docs/measurements/outbox-db-baseline.md</c>（A/B 用 A 基线）。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
[Trait("Category", "Measurement")]
public sealed class OutboxDbMeasurementTests
{
    private const int MessageCount = 2_000;
    private const int Seed = 20260813;

    // 时间敏感测量套件串行执行，避免并行线程池饥饿干扰耗时归因。
    [Fact]
    public async Task SaveAsync_HotPath_ProducesPgStatBaseline()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunSaveWorkloadAsync(200, Seed);

        var before = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 1);
        var after = await harness.SnapshotAsync();

        var inboundDiff = PostgresPerfDiffCalculator.Diff(before, after);

        // 断言窗口确实推进：产生了调用、行数与 WAL 字节，避免空转窗口被当作"可重复"基线。
        Assert.True(before.Statements.Count > 0, "预热后 pg_stat_statements 应有语句统计。");
        Assert.True(inboundDiff.Wal.WalBytes > 0, "热路径应产生 WAL 写入。");
        Assert.True(inboundDiff.Statements.Sum(s => s.Calls) > 0, "热路径应产生 SQL 调用。");

        // 追加 outbox 排水成本（claim + complete）。
        var drainBefore = await harness.SnapshotAsync();
        var completed = await harness.RunOutboxDrainAsync(batches: MessageCount / 50, batchSize: 50);
        var drainAfter = await harness.SnapshotAsync();
        var drainDiff = PostgresPerfDiffCalculator.Diff(drainBefore, drainAfter);
        Assert.True(completed > 0, "排水应完成至少一批消息。");

        await WriteBaselineReportAsync(inboundDiff, drainDiff, completed);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证 outbox fillfactor 对排水 WAL 的影响。
    /// <para>
    /// claim 只更新非索引列（locked_by/claim_token/locked_until_ms/attempt_count），理论上可 HOT；
    /// complete 改变 published_at_ms（<c>ix_outbox_pending</c> 部分索引谓词列）必然 non-HOT。
    /// 若 claim 因页内空间不足（fillfactor 过高）而 non-HOT，调低 fillfactor 应提升 HOT 命中、
    /// 降低排水语句级 WAL。以 pg_stat_statements 的语句级 wal_bytes 为权威归因。
    /// </para>
    /// </summary>
    [Fact]
    public async Task OutboxDrain_FillfactorAb_ReportsWalDiff()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热并排空，避免残留 pending 干扰后续窗口归因。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxDrainAsync(batches: 4, batchSize: 50);

        // A：fillfactor 90（Migration056 当前默认）。
        await harness.SetOutboxFillfactorAsync(90);
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 10);
        var aCompleted = await harness.RunOutboxDrainAsync(MessageCount / 50, batchSize: 50);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：fillfactor 75，预留更多页内空间给 HOT 版本链。
        await harness.SetOutboxFillfactorAsync(75);
        var bBefore = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 20);
        var bCompleted = await harness.RunOutboxDrainAsync(MessageCount / 50, batchSize: 50);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aCompleted > 0, "A 配置应完成至少一批排水。");
        Assert.True(bCompleted > 0, "B 配置应完成至少一批排水。");

        var aClaim = PerMessageWalBytes(aDiff, isClaim: true);
        var aComplete = PerMessageWalBytes(aDiff, isClaim: false);
        var bClaim = PerMessageWalBytes(bDiff, isClaim: true);
        var bComplete = PerMessageWalBytes(bDiff, isClaim: false);

        await WriteFillfactorAbReportAsync(
            aClaim, aComplete, (int)aCompleted,
            bClaim, bComplete, (int)bCompleted);
    }

    private static long PerMessageWalBytes(PostgresPerfDiff diff, bool isClaim)
    {
        // pg_stat_statements 将参数占位化为 $1/$2，因此按稳定的 SQL 文本而非参数名匹配。
        var matches = diff.Statements.Where(s => isClaim
            ? s.Query.Contains("candidates AS MATERIALIZED", StringComparison.OrdinalIgnoreCase)
              && s.Query.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
            : s.Query.Contains("published_at_ms", StringComparison.OrdinalIgnoreCase)
              && s.Query.Contains("UNNEST", StringComparison.OrdinalIgnoreCase));
        var rows = matches.Sum(s => s.Rows);
        return rows > 0 ? matches.Sum(s => s.WalBytes) / rows : 0;
    }

    private static async Task WriteFillfactorAbReportAsync(
        long aClaim,
        long aComplete,
        int aCompleted,
        long bClaim,
        long bComplete,
        int bCompleted)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-fillfactor-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 fillfactor A/B（排水路径 WAL）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、排水 {MessageCount} 条。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | claim WAL/消息 | complete WAL/消息 | 排水合计 WAL/消息 |");
        sb.AppendLine("|---|---|---|---|");
        sb.AppendLine($"| fillfactor=90（A） | {aClaim:N0} | {aComplete:N0} | {aClaim + aComplete:N0} |");
        sb.AppendLine($"| fillfactor=75（B） | {bClaim:N0} | {bComplete:N0} | {bClaim + bComplete:N0} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes / rows 归因，A/B 同容器顺序运行，仅 fillfactor 不同。");

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    private static async Task WriteBaselineReportAsync(
        PostgresPerfDiff inboundDiff,
        PostgresPerfDiff drainDiff,
        long completed)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-baseline.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 测量基线（PG 级 A/B）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"inbound 窗口 {MessageCount} 条消息。");
        sb.AppendLine();
        sb.AppendLine("## Inbound 热路径（SaveAsync：message + conversation/unread + outbox insert）");
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(inboundDiff, MessageCount, "Inbound 热路径"));
        sb.AppendLine();
        sb.AppendLine("## Outbox 排水（claim + complete）");
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(drainDiff, (int)Math.Max(1, completed), "Outbox 排水"));
        sb.AppendLine();
        sb.AppendLine(
            "## 说明\n\n- 本报告只记录经读写路径的 SQL/WAL 总量，不含任何消息正文、附件地址或凭据。\n" +
            "- 热路径每消息成本 = 窗口增量 / 消息数；A/B 时以同一语料/种子重跑，对比各条 SQL 的 calls/time/rows/wal_bytes 是否下降。\n");

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    private static string ResolveDocsMeasurementsDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "ChatApp.RealtimeServices");
            if (Directory.Exists(Path.Combine(candidate, "docs")))
            {
                return Path.Combine(candidate, "docs", "measurements");
            }

            dir = dir.Parent;
        }

        return Path.Combine(Directory.GetCurrentDirectory(), "docs", "measurements");
    }
}
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
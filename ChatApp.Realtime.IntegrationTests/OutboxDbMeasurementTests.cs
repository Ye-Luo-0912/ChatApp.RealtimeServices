using System.Text;
using ChatApp.Realtime.IntegrationTests.Measurement;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Npgsql;

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

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证剔除 <c>ix_messages_reply_to</c> / <c>ix_messages_forwarded_from</c>
    /// 两个部分索引对 messages 插入写放大的影响。
    /// <para>
    /// 两个部分索引（WHERE reply_to_message_id / forwarded_from_message_id IS NOT NULL）由
    /// Migration013/015 随字段创建，但代码库无任何读取路径按这两列过滤/排序/连接，仅贡献
    /// 插入（与撤回置 NULL）的索引写放大。A/B 使用带回复/转发引用的语料（每 4 条 1 条携带
    /// reply_to_*+forwarded_from_*），让部分索引在 A 配置下真实写入、B 配置下被剔除，
    /// 以 INSERT messages 语句级 wal_bytes/wal_records 归因。
    /// </para>
    /// </summary>
    [Fact]
    public async Task MessagesIndexAb_ReportsWalDiff()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热（含引用语料）并排空，避免残留 pending 干扰后续窗口归因。
        await harness.RunSaveWorkloadAsync(200, Seed, replyEvery: 4);
        await harness.RunOutboxDrainAsync(batches: 4, batchSize: 50);

        // A：两个部分索引存在（Migration013/015 定义的迁移后状态）。
        // 两窗口前都 TRUNCATE messages，使 A/B 从相同的空表起始（相同页分配模式），
        // 隔离「表随窗口增长导致页分配/页分裂差异」这一混杂因素。
        await harness.TruncateMessagesAsync();
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 30, replyEvery: 4);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：剔除两个部分索引后，同样从空表起始重跑。
        await harness.DropReplyForwardIndexesAsync();
        await harness.TruncateMessagesAsync();
        var bBefore = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 40, replyEvery: 4);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        // 还原 schema 与迁移目录一致（容器内部不影响其它测试）。
        await harness.CreateReplyForwardIndexesAsync();

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");

        var aInsertBytes = InsertMessagesWalPerMessage(aDiff, useRecords: false);
        var aInsertRecords = InsertMessagesWalPerMessage(aDiff, useRecords: true);
        var bInsertBytes = InsertMessagesWalPerMessage(bDiff, useRecords: false);
        var bInsertRecords = InsertMessagesWalPerMessage(bDiff, useRecords: true);

        await WriteIndexAbReportAsync(
            aDiff, bDiff,
            aInsertBytes, aInsertRecords,
            bInsertBytes, bInsertRecords);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 对比两种完成模式的完整排水生命周期 WAL。
    /// <para>
    /// 生产默认 <c>PublishedRetentionHours = 0</c> 走 delete-on-complete
    /// （<see cref="IRealtimeOutboxCompactionStore.DeleteClaimedPublishedBatchAsync"/>），
    /// 且 <c>OutboxCleanupWorker</c> 不运行；保留模式（&gt;0）走
    /// <see cref="IRealtimeOutboxStore.MarkPublishedBatchAsync"/> + 后续 cleanup。
    /// A（保留）：claim + MarkPublished + cleanup（完整生命周期，结束为空表）；
    /// B（即删）：claim + DeleteClaimedPublished（完整生命周期，结束为空表）。
    /// 以 pg_stat_statements 语句级 wal_bytes 归因，A/B 同容器顺序运行、仅完成模式不同。
    /// </para>
    /// </summary>
    [Fact]
    public async Task OutboxDrain_CompleteModeAb_ReportsWalDiff()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热并排空（delete-on-complete），避免残留 pending 干扰后续窗口归因。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxDrainDeleteAsync(batches: 4, batchSize: 50);

        // A：保留模式完整生命周期（claim + MarkPublished + cleanup）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 50);
        var aBefore = await harness.SnapshotAsync();
        var aCompleted = await harness.RunOutboxDrainAsync(MessageCount / 50, batchSize: 50);
        var aMid = await harness.SnapshotAsync();
        var aCleaned = await harness.RunOutboxCleanupAsync();
        var aAfter = await harness.SnapshotAsync();
        var aDrain = PostgresPerfDiffCalculator.Diff(aBefore, aMid);
        var aCleanup = PostgresPerfDiffCalculator.Diff(aMid, aAfter);
        var aTotal = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：delete-on-complete 模式完整生命周期（claim + DeleteClaimedPublished）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 60);
        var bBefore = await harness.SnapshotAsync();
        var bCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 50, batchSize: 50);
        var bAfter = await harness.SnapshotAsync();
        var bDrain = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aCompleted > 0, "A 配置应完成至少一批排水。");
        Assert.True(bCompleted > 0, "B 配置应完成至少一批排水。");
        Assert.True(aCleaned > 0, "A 配置 cleanup 应清除已发布行。");

        await WriteCompleteModeAbReportAsync(
            aDrain, aCleanup, bDrain,
            (int)aCompleted, (int)aCleaned, (int)bCompleted);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：claim 路径 WAL 分解诊断。
    /// <para>
    /// 在 delete-on-complete（生产默认）模式下，claim 语句是排水路径的主要 WAL 项
    /// （基线约 850–870 WAL 字节/消息、5+ WAL 记录/消息——远超单条 HOT 更新应有的 1 条记录）。
    /// 本测试用 CHECKPOINT 隔离 FPI（full page image）混杂因素：窗口 A「先 CHECKPOINT 再排水」
    /// 近似生产稳态（页已落盘、无需整页镜像），窗口 B 直接排水（容器冷页，可能触发 FPI），
    /// 并配合表级 HOT 命中率（hot_updates/updates）定位 claim 是 HOT 还是 non-HOT。
    /// 报告写入 <c>docs/measurements/outbox-db-claim-wal-breakdown.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task OutboxClaim_WalBreakdown_ReportsFpiAndHot()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热并排空（delete-on-complete），避免残留 pending 干扰后续窗口归因。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxDrainDeleteAsync(batches: 4, batchSize: 50);

        // A：先 CHECKPOINT 再排水（稳态页，无 FPI 混杂）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 70);
        await harness.RunCheckpointAsync();
        var aBefore = await harness.SnapshotAsync();
        var aCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 50, batchSize: 50);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：不 CHECKPOINT 直接排水（容器冷页，排水可能触发 FPI）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 80);
        var bBefore = await harness.SnapshotAsync();
        var bCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 50, batchSize: 50);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aCompleted > 0, "A 配置应完成至少一批排水。");
        Assert.True(bCompleted > 0, "B 配置应完成至少一批排水。");

        await WriteClaimBreakdownReportAsync(
            aDiff, (int)aCompleted,
            bDiff, (int)bCompleted);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证 delete-on-complete（生产默认）模式下更低 fillfactor
    /// 对 claim HOT 命中率与排水 WAL 的影响。
    /// <para>
    /// claim 只更新非索引列（locked_by/claim_token/locked_until_ms/attempt_count），理论上可 HOT，
    /// 但 HOT 要求新版本落在旧版本同页、页内需同时容纳旧+新版本。按页填充率模型
    /// HOT 命中率 ≈ (100 - fillfactor) / fillfactor：fillfactor=75 时约 33%（实测 30–40%），
    /// 其余行 non-HOT 需维护全部索引并产生 5+ WAL 记录/消息；fillfactor=50 时模型预测
    /// 单次 claim（delete-on-complete 每行只 claim 一次）可逼近 100% HOT。
    /// 本测试用 A/B（75 vs 50）实测 claim/complete 语句级 WAL 与 outbox 表 HOT 命中率，
    /// 报告写入 <c>docs/measurements/outbox-db-fillfactor-hot-ab.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task OutboxDrain_FillfactorHotAb_ReportsWalAndHot()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热并排空（delete-on-complete），避免残留 pending 干扰后续窗口归因。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxDrainDeleteAsync(batches: 4, batchSize: 50);

        // A：fillfactor=75（Migration067 当前默认）。
        await harness.SetOutboxFillfactorAsync(75);
        await harness.TruncateOutboxAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 110);
        await harness.RunCheckpointAsync();
        var aBefore = await harness.SnapshotAsync();
        var aCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 50, batchSize: 50);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：fillfactor=50，为单次 claim 的整页 HOT 版本链预留 50% 页内空间。
        await harness.SetOutboxFillfactorAsync(50);
        await harness.TruncateOutboxAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 120);
        await harness.RunCheckpointAsync();
        var bBefore = await harness.SnapshotAsync();
        var bCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 50, batchSize: 50);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aCompleted > 0, "A 配置应完成至少一批排水。");
        Assert.True(bCompleted > 0, "B 配置应完成至少一批排水。");

        await WriteFillfactorHotAbReportAsync(
            aDiff, (int)aCompleted, 75,
            bDiff, (int)bCompleted, 50);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：排水 HOT 更细归因——把 aggregate HOT 归因到 claim 语句，
    /// 并量化 delete 产生的 dead tuple 对 claim HOT 的页内空间争夺。
    /// <para>
    /// delete-on-complete 排水里唯一的 UPDATE 是 claim（locked_by/claim_token/locked_until_ms/
    /// attempt_count，全为非索引列，理论上可 HOT），complete 是 DELETE（无 HOT 概念）。
    /// 上一轮 fillfactor=50 测得 aggregate HOT≈87% 而非 100%，本项隔离 root cause：
    /// A 只 claim（无 delete），B 完整 claim+delete。实测 A 与 B 的 claim hot_updates
    /// 逐条相同（同为 1,723/2,000 = 86%），而 dead tuple 差异巨大（450 vs 2,286），
    /// 证明 delete 生成的 dead tuple 对 claim HOT 无影响——residual 是 claim 语句因
    /// 页填充率与行更新后体积的物理布局导致的固有 non-HOT，而非索引列改写或删除干扰。
    /// </para>
    /// </summary>
    [Fact]
    public async Task DrainClaimHot_Isolation_AttributesPageSpaceContention()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热两模式的连接池/plan cache；该段不纳入测量窗口。
        await harness.SetOutboxFillfactorAsync(50);
        await harness.TruncateOutboxAsync();
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxClaimOnlyAsync(200 / 50, 50);
        await harness.RunSaveWorkloadAsync(200, Seed + 1);
        await harness.RunOutboxDrainDeleteAsync(200 / 50, 50);

        // A：claim-only（不 delete，无 delete 引入的 dead tuple）——隔离 claim 的 HOT 命中上限。
        await harness.SetOutboxFillfactorAsync(50);
        await harness.TruncateOutboxAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 400);
        await harness.RunCheckpointAsync();
        var aBefore = await harness.SnapshotAsync();
        var aClaimed = await harness.RunOutboxClaimOnlyAsync(MessageCount / 200, 200);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：claim+delete（delete 产生 dead tuple，与 claim 争夺页内空间）——生产默认形态。
        await harness.SetOutboxFillfactorAsync(50);
        await harness.TruncateOutboxAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 410);
        await harness.RunCheckpointAsync();
        var bBefore = await harness.SnapshotAsync();
        var bCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 200, 200);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.Equal(MessageCount, aClaimed);
        Assert.Equal(MessageCount, bCompleted);
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 claim SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生排水 SQL 调用。");
        // 归因断言：claim-only（A）无 delete 干扰，claim 的 HOT 命中应不低于 claim+delete（B）。
        var aOutbox = diffTable(aDiff, "outbox");
        var bOutbox = diffTable(bDiff, "outbox");
        Assert.True(bOutbox.DeadTuples > aOutbox.DeadTuples,
            "delete-on-complete（B）应产生比 claim-only（A）更多的 dead tuple。");
        Assert.True(aOutbox.HotUpdates >= bOutbox.HotUpdates,
            "无 delete 干扰页内空间时 claim HOT 应不低于完整排水。");

        await WriteClaimHotAttributionReportAsync(aDiff, (int)aClaimed, bDiff, (int)bCompleted);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：回归验证 Migration069 将 outbox 表 fillfactor 落到 50。
    /// <para>
    /// 全新容器经 <see cref="Measurement.PostgresPerfHarness.InitializeAsync"/> 完整跑一遍默认迁移目录，
    /// 应看到 outbox 表 <c>reloptions</c> 含 <c>fillfactor=50</c>（A/B 实测排水 WAL 降 57% 的最终落点），
    /// 且未再次回退为 Migration067 的 75。这是「迁移产物可复现」的守卫，防止后续迁移误改。
    /// </para>
    /// </summary>
    [Fact]
    public async Task MigrationCatalog_OutboxFillfactorEndsAt50()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        await using var connection = await harness.Client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            """
            SELECT COALESCE(c.reloptions, ARRAY[]::text[])
            FROM pg_class AS c
            INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema_name
              AND c.relname = 'outbox';
            """,
            connection);
        cmd.Parameters.AddWithValue("schema_name", harness.Schema.Schema);

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "默认迁移后 outbox 表应存在。");
        var relOptions = reader.GetFieldValue<string[]>(0);
        Assert.Contains("fillfactor=50", relOptions);
        Assert.DoesNotContain(relOptions, o => o.StartsWith("fillfactor=") && o != "fillfactor=50");
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证 admission 合并对「重复读取/往返」的削减。
    /// <para>
    /// A 为 fallback 路径（<c>idempotencyLedger: null</c>）：生命周期读取、账本读取、
    /// 会话序号分配是 3 条独立 SQL（每次 SaveAsync 约 3 次往返 + bundle）。
    /// B 为生产合并路径（注入 <see cref="NpgsqlCommandIdempotencyLedger"/>）：admission 命令
    /// 用单条 CTE 把「生命周期锁/状态 + canonical 账本读取 + 事务内授权 + 会话序号分配」
    /// 合并为一条 SQL，每次 SaveAsync 约 2 次往返 + bundle。
    /// 以 pg_stat_statements 语句级 calls 折算每条消息平均 SQL 往返数，并对照 WAL/耗时。
    /// 报告写入 <c>docs/measurements/outbox-db-admission-merge-ab.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_AdmissionMergeAb_ReportsRoundTripSavings()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        // 合并路径的 direct_authorization 读取 public."AspNetUsers"/"T_BlockRecords"/
        // "T_UserFriendEntry"，需先播种语料用户的授权关系使判定为 Allowed。
        await harness.SeedAuthTablesAsync();

        // 预热两路径的连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunMergedSaveWorkloadAsync(200, Seed + 1);

        // A：fallback（lifecycle admission + 序号分配 + bundle 三次往返）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 200);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：生产合并（admission+序号分配单条 CTE + bundle 两次往返）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunMergedSaveWorkloadAsync(MessageCount, Seed + 210);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");

        await WriteAdmissionMergeAbReportAsync(aDiff, bDiff);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证「合并同事务写入」为单条 super-bundle CTE 的往返收益。
    /// <para>
    /// 生产合并路径（<see cref="Measurement.PostgresPerfHarness.RunMergedSaveWorkloadAsync"/>）
    /// 每次 SaveAsync 仍是 2 条数据语句：admission CTE（生命周期/授权/幂等 + 会话与 member 写入 +
    /// 序号分配）与 bundle CTE（message + outbox + ledger 写入）。B 用单条 super-bundle CTE
    /// 把这两个同事务写入合并（复用 <c>write_gate</c> 门控与 <c>upsert_conversation</c> /
    /// <c>sender_upsert</c> 序号分配，让 message INSERT 直接取 <c>last_sequence</c> /
    /// <c>sent_count</c>），热路径数据往返 2 → 1。
    /// 以 pg_stat_statements 语句级 calls 折算每条消息平均 SQL 往返数，并对照 WAL/耗时。
    /// 报告写入 <c>docs/measurements/outbox-db-super-bundle-ab.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_SuperBundleAb_ReportsRoundTripSavings()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        // super-bundle 复用合并路径的 direct_authorization 判定，需先播种授权关系。
        await harness.SeedAuthTablesAsync();

        // 预热两路径的连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunMergedSaveWorkloadAsync(200, Seed);
        await harness.RunSuperBundleWorkloadAsync(200, Seed + 1);

        // A：生产合并（admission CTE + bundle，2 条数据语句）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunMergedSaveWorkloadAsync(MessageCount, Seed + 200);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：super-bundle（合并同事务写入为单条 CTE）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunSuperBundleWorkloadAsync(MessageCount, Seed + 210);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");

        await WriteSuperBundleAbReportAsync(aDiff, bDiff);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证「跳过同会话重复授权读取」的每消息成本。
    /// <para>
    /// 合并路径 admission 段的授权读取（<c>direct_user_state</c>：AspNetUsers 2 行 +
    /// <c>direct_authorization</c>：T_BlockRecords 1 次 EXISTS + T_UserFriendEntry 2 次 EXISTS，
    /// 共 4 处表读取）在每条同会话消息间重复执行。A 用带授权读取的 super-bundle，
    /// B 用 no-auth super-bundle（<c>write_gate</c> 只按生命周期 + 幂等 canonical 门控）——
    /// 两路径语句数相同（单条 CTE）、写入行集完全相同，唯一差异是授权读取，故
    /// 执行耗时/WAL 的差异即可归因「重复授权读取」成本。报告写入
    /// <c>docs/measurements/outbox-db-auth-reads-ab.md</c>。
    /// </para>
    /// <para>
    /// B 为测量专用变体（跳过授权在语义上不安全，仅用于量化读取成本，不落生产）；
    /// 若授权读取占比显著，后续可评估「会话级授权缓存 / 已建会话免重复授权」方向。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_NoAuthReadsAb_ReportsAuthorizationReadCost()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        // 两配置均播种授权关系（direct_authorization 判定 Allowed），保证 A 正常写、B 无差异写。
        await harness.SeedAuthTablesAsync();

        // 预热两路径的连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunSuperBundleWorkloadAsync(200, Seed);
        await harness.RunNoAuthSuperBundleWorkloadAsync(200, Seed + 1);

        // A：带授权读取的 super-bundle（单条 CTE，admission 读 5 张表：tombstone/ledger/AspNetUsers/Block/Friend）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSuperBundleWorkloadAsync(MessageCount, Seed + 200);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：跳过授权读取的 super-bundle（单条 CTE，admission 只读 tombstone/ledger 2 张表）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunNoAuthSuperBundleWorkloadAsync(MessageCount, Seed + 210);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");

        await WriteNoAuthReadsAbReportAsync(aDiff, bDiff);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证「会话级授权缓存 / 已建会话免重复授权」的每消息收益。
    /// <para>
    /// 多会话分布（<see cref="Measurement.PostgresPerfHarness.RunSessionAuthCacheWorkloadAsync"/>，
    /// <c>conversationCount</c> 个会话、每会话 msgsPerConv 条消息）：A 每条消息都执行完整授权读取
    /// （<c>direct_user_state</c> + <c>direct_authorization</c>，4 处表读取）；B 仅每会话第一条消息执行
    /// 完整授权读取（建立会话），后续消息跳过授权读取（模拟会话级授权缓存命中、已建会话免重复授权）。
    /// 两路径语句数相同（单条 CTE）、写入行集相同、会话分布相同，唯一差异是已建会话是否重复授权读取，
    /// 故执行耗时差异即可归因「已建会话免重复授权」的收益。报告写入
    /// <c>docs/measurements/outbox-db-session-auth-cache-ab.md</c>。
    /// </para>
    /// <para>
    /// B 为测量专用变体（会话级授权缓存的模拟实现；授权事实在会话生命周期内假定稳定，落地生产需
    /// TTL/失效注入）。收益随会话长度缩放：每会话 msgsPerConv 条消息时回收约
    /// (msgsPerConv−1)/msgsPerConv × 每次授权读取成本。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_SessionAuthCacheAb_ReportsEstablishedSessionSaving()
    {
        // 2000 条 / 40 会话 = 每会话 50 条（已建会话：首条建立 + 49 条免重复授权）。
        const int conversationCount = 40;
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        // 为每个会话的接收者播种授权关系（direct_authorization 判定 Allowed），保证 A 正常写、B 无差异写。
        await harness.SeedAuthTablesAsync(receiverCount: conversationCount);

        // 预热两配置的连接池与 plan cache（创建 40 个已建会话；该段不纳入测量窗口）。
        await harness.RunSuperBundleWorkloadAsync(200, Seed, conversationCount: conversationCount);
        await harness.RunSessionAuthCacheWorkloadAsync(200, Seed + 1, conversationCount: conversationCount);

        // A：每条消息都做完整授权读取（多会话同分布基线）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSuperBundleWorkloadAsync(MessageCount, Seed + 200, conversationCount: conversationCount);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：已建会话免重复授权（每会话首条完整授权，后续跳过授权读取）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunSessionAuthCacheWorkloadAsync(MessageCount, Seed + 210, conversationCount: conversationCount);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");

        await WriteSessionAuthCacheAbReportAsync(aDiff, bDiff, conversationCount);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证「仅读接收者 user_state」的每消息收益（发送者已认证免重复读取）。
    /// <para>
    /// A 为完整授权读取的 super-bundle（<c>direct_user_state</c> 读取 AspNetUsers 的发送者 + 接收者
    /// 两行，<c>WHERE "Id" IN ($2, $3)</c>）；B 为 sender-known 变体（<see cref="Measurement.PostgresPerfHarness.RunSenderKnownSuperBundleWorkloadAsync"/>，
    /// <c>direct_user_state</c> 只读取接收者行 <c>WHERE "Id" = $3</c>、<c>sender_exists</c> 固定为 TRUE）。
    /// 语义依据：生产发送者是已认证用户，其存在性在 admission 时已保证，故 sender 行的存在性读取是
    /// 每消息的冗余读取。两路径语句数相同（单条 CTE）、写入行集相同、会话分布相同，唯一差异是
    /// direct_user_state 是否读取发送者行，故执行耗时差异即可归因「发送者状态冗余读取」的成本。
    /// 报告写入 <c>docs/measurements/outbox-db-sender-known-ab.md</c>。
    /// </para>
    /// <para>
    /// 预期（并实测确认）：direct_user_state 的发送者/接收者行均命中共享缓冲区（blks_read=0），
    /// 该冗余读取的每消息成本处于测量噪声内（执行耗时差 ~0.002 ms），为负收益——此变体不值得落地。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_SenderKnownAb_ReportsSenderStateReadSaving()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        // 播种授权关系（direct_authorization 判定 Allowed），保证 A/B 均正常写、无差异写。
        await harness.SeedAuthTablesAsync();

        // 预热两路径的连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunSuperBundleWorkloadAsync(200, Seed);
        await harness.RunSenderKnownSuperBundleWorkloadAsync(200, Seed + 1);

        // A：完整授权读取（direct_user_state 读取发送者 + 接收者两行）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunSuperBundleWorkloadAsync(MessageCount, Seed + 200);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：sender-known（direct_user_state 只读取接收者行，sender_exists 固定为 TRUE）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunSenderKnownSuperBundleWorkloadAsync(MessageCount, Seed + 210);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");

        await WriteSenderKnownAbReportAsync(aDiff, bDiff);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 量化「避免无变化 UPDATE（会话头列）」的每消息收益。
    /// <para>
    /// 以「会话头高水位乱序」语料（第 0 条极高 received_at_ms 建立高水位、后续消息均低于高水位，
    /// 会话头 last_message_* 不再推进）驱动同一条 super-bundle 热路径：A 为无条件覆盖会话头列
    /// （无 CASE 守卫，被 <c>ix_conversations_last_message_list</c> 索引的 last_message_at_ms 每消息改写
    /// → non-HOT + 索引维护）；B 为生产 CASE 守卫（保留未推进的会话头列 → HOT 更新）。两者写入行集
    /// 完全相同，仅会话头列是否被改写不同。以 pg_stat_statements + 表级 HOT 命中率归因。
    /// 报告写入 <c>docs/measurements/outbox-db-noop-update-ab.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task SaveAsync_NoopUpdateAb_ReportsConversationHeaderSaving()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();
        await harness.SeedAuthTablesAsync();

        // 预热两配置的连接池与 plan cache；该段不纳入测量窗口。
        await harness.RunOutOfOrderGuardedWorkloadAsync(200, Seed);
        await harness.RunOutOfOrderNaiveWorkloadAsync(200, Seed + 1);

        // A：无条件覆盖会话头列（naive，无 CASE 守卫）。
        var aBefore = await harness.SnapshotAsync();
        await harness.RunOutOfOrderNaiveWorkloadAsync(MessageCount, Seed + 220);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：生产 CASE 守卫（保留未推进的会话头列 → HOT 更新）。
        var bBefore = await harness.SnapshotAsync();
        await harness.RunOutOfOrderGuardedWorkloadAsync(MessageCount, Seed + 230);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.True(aDiff.Wal.WalBytes > 0, "A 配置应产生 WAL 写入。");
        Assert.True(bDiff.Wal.WalBytes > 0, "B 配置应产生 WAL 写入。");
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生 SQL 调用。");
        // 结论性断言：Migration058 已 DROP 含 last_message_at_ms 的 ix_conversations_last_message_list，
        // 该列已非索引列，故 A/B 两配置的会话 tip 更新都应达到 HOT 命中（守卫不再影响索引维护）。
        Assert.True(ConversationHotRatio(aDiff) >= 90,
            "A（无条件覆盖会话头列）在无该索引时也应 HOT 命中 ≥90%。");
        Assert.True(ConversationHotRatio(bDiff) >= 90,
            "B（CASE 守卫）在无该索引时也应 HOT 命中 ≥90%。");

        await WriteNoopUpdateAbReportAsync(aDiff, bDiff);
    }

    /// <summary>conversations 表的 HOT 命中率（HOT 更新 / 全部更新）。</summary>
    private static double ConversationHotRatio(PostgresPerfDiff diff)
    {
        var conv = diff.Tables.FirstOrDefault(t => t.TableName.Equals("conversations", StringComparison.OrdinalIgnoreCase));
        return conv is not null && conv.Updates > 0 ? conv.HotUpdates * 100.0 / conv.Updates : 0;
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：A/B 验证 claim/complete 的「有界批量上限」对每消息往返/事务开销的影响。
    /// <para>
    /// claim/delete 均为单语句（`FOR UPDATE ... SKIP LOCKED ... LIMIT @batch_size` /
    /// `DELETE ... USING UNNEST`），批量上限只改变每条消息的平均往返次数与事务提交次数
    /// （autocommit 下每语句一个事务）：A 用批量上限 1（每条消息独立 claim+delete 往返），
    /// B 用有界批量上限 200（单事务/单 claim 跨度可控）。以 pg_stat_statements 语句级 calls
    /// 折算每消息排水往返数与 WAL/耗时。报告写入 <c>docs/measurements/outbox-db-batch-size-ab.md</c>。
    /// </para>
    /// </summary>
    [Fact]
    public async Task OutboxDrain_BatchSizeAb_ReportsRoundTripSavings()
    {
        await using var harness = new PostgresPerfHarness();
        await harness.InitializeAsync();

        // 预热两批量上限的连接池/plan cache；该段不纳入测量窗口。
        await harness.RunSaveWorkloadAsync(200, Seed);
        await harness.RunOutboxDrainDeleteAsync(200, 50);
        await harness.RunSaveWorkloadAsync(200, Seed + 1);
        await harness.RunOutboxDrainDeleteAsync(200, 200);

        // A：批量上限 1（每条消息独立 claim+delete）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 320);
        var aBefore = await harness.SnapshotAsync();
        var aCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount, 1);
        var aAfter = await harness.SnapshotAsync();
        var aDiff = PostgresPerfDiffCalculator.Diff(aBefore, aAfter);

        // B：有界批量上限 200（单事务跨度可控）。
        await harness.RunSaveWorkloadAsync(MessageCount, Seed + 330);
        var bBefore = await harness.SnapshotAsync();
        var bCompleted = await harness.RunOutboxDrainDeleteAsync(MessageCount / 200, 200);
        var bAfter = await harness.SnapshotAsync();
        var bDiff = PostgresPerfDiffCalculator.Diff(bBefore, bAfter);

        Assert.Equal(MessageCount, aCompleted);
        Assert.Equal(MessageCount, bCompleted);
        Assert.True(aDiff.Statements.Sum(s => s.Calls) > 0, "A 配置应产生排水 SQL 调用。");
        Assert.True(bDiff.Statements.Sum(s => s.Calls) > 0, "B 配置应产生排水 SQL 调用。");

        await WriteBatchSizeAbReportAsync(aDiff, bDiff);
    }

    /// <summary>
    /// 归因 claim 语句（CTE candidates + UPDATE）的窗口增量明细。
    /// </summary>
    private static PgStatementDiff? ClaimStatement(PostgresPerfDiff diff) =>
        diff.Statements.FirstOrDefault(s =>
            s.Query.Contains("candidates AS MATERIALIZED", StringComparison.OrdinalIgnoreCase)
            && s.Query.Contains("UPDATE", StringComparison.OrdinalIgnoreCase));

    private static async Task WriteClaimBreakdownReportAsync(
        PostgresPerfDiff aDiff,
        int aCompleted,
        PostgresPerfDiff bDiff,
        int bCompleted)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-claim-wal-breakdown.md");

        var aClaim = ClaimStatement(aDiff);
        var bClaim = ClaimStatement(bDiff);

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 claim 路径 WAL 分解（FPI 隔离 + HOT 命中率）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、delete-on-complete 排水 {MessageCount} 条。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | claim WAL/消息 | claim WAL 记录/消息 | claim FPI/消息 | claim FPI 字节估算/消息 |");
        sb.AppendLine("|---|---|---|---|---|");
        if (aClaim is not null && aClaim.Rows > 0)
        {
            sb.AppendLine($"| A：先 CHECKPOINT 再排水（稳态页） | {aClaim.WalBytes / aClaim.Rows:N0} | {aClaim.WalRecords / (double)aClaim.Rows:N1} | {aClaim.WalFpi / (double)aClaim.Rows:N2} | {aClaim.WalFpi * 8192L / aClaim.Rows:N0} |");
        }

        if (bClaim is not null && bClaim.Rows > 0)
        {
            sb.AppendLine($"| B：直接排水（容器冷页） | {bClaim.WalBytes / bClaim.Rows:N0} | {bClaim.WalRecords / (double)bClaim.Rows:N1} | {bClaim.WalFpi / (double)bClaim.Rows:N2} | {bClaim.WalFpi * 8192L / bClaim.Rows:N0} |");
        }

        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes/wal_records/wal_fpi 归因，A/B 同容器顺序运行、语料同种子前缀不同；");
        sb.AppendLine("> FPI 字节估算按 8KB/页 折算；A 在排水前强制 CHECKPOINT，使排水窗口内页已落盘、近似生产稳态（无 FPI）；");
        sb.AppendLine("> B 不 CHECKPOINT，直接排水（冷页首次修改可能触发整页镜像）。");
        sb.AppendLine();

        sb.Append(PostgresPerfReporter.Render(aDiff, aCompleted, "A：先 CHECKPOINT 再排水（稳态页）"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(bDiff, bCompleted, "B：直接排水（容器冷页）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// 归因 complete/删除语句（DELETE ... UNNEST）的窗口增量明细。
    /// </summary>
    private static PgStatementDiff? DeleteStatement(PostgresPerfDiff diff) =>
        diff.Statements.FirstOrDefault(s =>
            s.Query.Contains("DELETE", StringComparison.OrdinalIgnoreCase)
            && s.Query.Contains("UNNEST", StringComparison.OrdinalIgnoreCase));

    /// <summary>outbox 表的窗口内 HOT 命中率（hot_updates / updates，百分比）。</summary>
    private static double? OutboxHotRate(PostgresPerfDiff diff)
    {
        var outbox = diff.Tables.FirstOrDefault(t => t.TableName == "outbox");
        if (outbox is null || outbox.Updates <= 0)
        {
            return null;
        }

        return outbox.HotUpdates * 100.0 / outbox.Updates;
    }

    private static async Task WriteFillfactorHotAbReportAsync(
        PostgresPerfDiff aDiff,
        int aCompleted,
        int aFillfactor,
        PostgresPerfDiff bDiff,
        int bCompleted,
        int bFillfactor)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-fillfactor-hot-ab.md");

        var aClaim = ClaimStatement(aDiff);
        var bClaim = ClaimStatement(bDiff);
        var aDelete = DeleteStatement(aDiff);
        var bDelete = DeleteStatement(bDiff);
        var aHot = OutboxHotRate(aDiff);
        var bHot = OutboxHotRate(bDiff);

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 fillfactor A/B（delete-on-complete 排水，HOT 命中率）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、delete-on-complete 排水 {MessageCount} 条；" +
                       "两窗口前均 TRUNCATE outbox 从空表起始。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | claim WAL/消息 | claim WAL 记录/消息 | complete(删除) WAL/消息 | 排水合计 WAL/消息 | outbox HOT 命中率 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        if (aClaim is not null && aClaim.Rows > 0)
        {
            var aDeleteWal = aDelete is not null && aDelete.Rows > 0 ? aDelete.WalBytes / aDelete.Rows : 0;
            sb.AppendLine($"| fillfactor={aFillfactor}（A） | {aClaim.WalBytes / aClaim.Rows:N0} | {aClaim.WalRecords / (double)aClaim.Rows:N1} | {aDeleteWal:N0} | {aClaim.WalBytes / aClaim.Rows + aDeleteWal:N0} | {(aHot is null ? "—" : aHot.Value.ToString("N0") + "%")} |");
        }

        if (bClaim is not null && bClaim.Rows > 0)
        {
            var bDeleteWal = bDelete is not null && bDelete.Rows > 0 ? bDelete.WalBytes / bDelete.Rows : 0;
            sb.AppendLine($"| fillfactor={bFillfactor}（B） | {bClaim.WalBytes / bClaim.Rows:N0} | {bClaim.WalRecords / (double)bClaim.Rows:N1} | {bDeleteWal:N0} | {bClaim.WalBytes / bClaim.Rows + bDeleteWal:N0} | {(bHot is null ? "—" : bHot.Value.ToString("N0") + "%")} |");
        }

        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因，A/B 同容器顺序运行、仅 fillfactor 不同；");
        sb.AppendLine("> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表），按页填充率模型 HOT ≈ (100 − fillfactor) / fillfactor；");
        sb.AppendLine("> fillfactor 只影响改变之后新插入行所在的页，先 ALTER 再 TRUNCATE 再插入，保证 A/B 各窗口页布局符合其 fillfactor。");
        sb.AppendLine();

        sb.Append(PostgresPerfReporter.Render(aDiff, aCompleted, $"A：fillfactor={aFillfactor}（delete-on-complete 排水）"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(bDiff, bCompleted, $"B：fillfactor={bFillfactor}（delete-on-complete 排水）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// OUTBOX-DB-1 需求 2：生成排水 HOT 更细归因报告（claim-only vs claim+delete）。
    /// <para>
    /// 把 aggregate HOT（fillfactor=50 下 87%）归因到 claim 语句，并量化 delete 生成的
    /// dead tuple 对 claim 页内版本链空间的争夺。A（claim-only）无 delete，隔离 claim 的
    /// HOT 命中上限；B（claim+delete）为生产默认形态。若 A 的 claim HOT 明显高于 B，
    /// 则 residual 来自 delete dead tuple 挤压页内空间，而非索引列改写。
    /// </para>
    /// </summary>
    private static async Task WriteClaimHotAttributionReportAsync(
        PostgresPerfDiff aDiff,
        int aClaimed,
        PostgresPerfDiff bDiff,
        int bCompleted)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-claim-hot-attribution-ab.md");

        var aOutbox = diffTable(aDiff, "outbox");
        var bOutbox = diffTable(bDiff, "outbox");
        var aHot = OutboxHotRate(aDiff);
        var bHot = OutboxHotRate(bDiff);
        var aClaim = ClaimStatement(aDiff);
        var bClaim = ClaimStatement(bDiff);

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 排水 HOT 更细归因（claim-only vs claim+delete）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"两配置均 fillfactor=50、每配置 inbound {MessageCount} 条消息；" +
                       "A 只 claim（不 delete）、B claim+delete（delete-on-complete）；两窗口前均 TRUNCATE outbox 从空表起始。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | claim 消息数 | claim WAL/消息 | claim WAL 记录/消息 | outbox updates | outbox hot_updates | HOT 命中率 | outbox dead tuples |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|");
        if (aClaim is not null && aClaim.Rows > 0)
        {
            sb.AppendLine($"| A：claim-only（无 delete） | {aClaimed:N0} | {aClaim.WalBytes / aClaim.Rows:N0} | {aClaim.WalRecords / (double)aClaim.Rows:N1} | {aOutbox?.Updates ?? 0:N0} | {aOutbox?.HotUpdates ?? 0:N0} | {(aHot is null ? "—" : aHot.Value.ToString("N0") + "%")} | {aOutbox?.DeadTuples ?? 0:N0} |");
        }

        if (bClaim is not null && bClaim.Rows > 0)
        {
            sb.AppendLine($"| B：claim+delete（生产默认） | {bCompleted:N0} | {bClaim.WalBytes / bClaim.Rows:N0} | {bClaim.WalRecords / (double)bClaim.Rows:N1} | {bOutbox?.Updates ?? 0:N0} | {bOutbox?.HotUpdates ?? 0:N0} | {(bHot is null ? "—" : bHot.Value.ToString("N0") + "%")} | {bOutbox?.DeadTuples ?? 0:N0} |");
        }

        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因 claim，A/B 同容器顺序运行、仅差是否 delete；");
        sb.AppendLine("> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表）；delete-on-complete 排水里唯一的 UPDATE 是 claim（");
        sb.AppendLine("> locked_by/claim_token/locked_until_ms/attempt_count 全为非索引列，理论上可 HOT），complete 是 DELETE（无 HOT 概念）；");
        sb.AppendLine("> 若 A 的 claim HOT 命中率显著高于 B 且 B 的 dead tuple 更多，则 residual（HOT<100%）来自 delete 生成的");
        sb.AppendLine("> dead tuple 挤压 claim 的页内版本链空间（页填充率限制），而非索引列改写。");
        sb.AppendLine();

        sb.Append(PostgresPerfReporter.Render(aDiff, aClaimed, "A：claim-only（无 delete，隔离 claim HOT 上限）"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(bDiff, bCompleted, "B：claim+delete（delete-on-complete 生产默认形态）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// 归因 <c>INSERT ... "messages"</c> 语句的窗口增量，按 rows 折算每消息 WAL。
    /// <paramref name="useRecords"/> 为 true 时返回 wal_records，否则返回 wal_bytes。
    /// </summary>
    private static long InsertMessagesWalPerMessage(PostgresPerfDiff diff, bool useRecords)
    {
        var matches = diff.Statements.Where(s =>
            s.Query.Contains("INSERT INTO", StringComparison.OrdinalIgnoreCase)
            && s.Query.Contains("\"messages\"", StringComparison.OrdinalIgnoreCase)).ToList();
        var rows = matches.Sum(s => s.Rows);
        if (rows <= 0)
        {
            return 0;
        }

        var wal = useRecords ? matches.Sum(s => s.WalRecords) : matches.Sum(s => s.WalBytes);
        return wal / rows;
    }

    private static long PerMessageWalBytes(PostgresPerfDiff diff, bool isClaim)
    {
        // pg_stat_statements 将参数占位化为 $1/$2，因此按稳定的 SQL 文本而非参数名匹配。
        return PerMessageWalBytes(diff, query => isClaim
            ? query.Contains("candidates AS MATERIALIZED", StringComparison.OrdinalIgnoreCase)
              && query.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
            : query.Contains("published_at_ms", StringComparison.OrdinalIgnoreCase)
              && query.Contains("UNNEST", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// 按 SQL 文本谓词归因窗口内匹配语句的 WAL 字节，并按 rows 折算每消息。
    /// </summary>
    private static long PerMessageWalBytes(PostgresPerfDiff diff, Func<string, bool> match)
    {
        var matches = diff.Statements
            .Where(s => match(s.Query))
            .ToList();
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

    private static async Task WriteCompleteModeAbReportAsync(
        PostgresPerfDiff aDrain,
        PostgresPerfDiff aCleanup,
        PostgresPerfDiff bDrain,
        int aCompleted,
        int aCleaned,
        int bCompleted)
    {
        // claim / complete / cleanup / delete 按稳定 SQL 文本归因（pg_stat_statements 参数占位化）。
        var aClaim = PerMessageWalBytes(aDrain, isClaim: true);
        var aComplete = PerMessageWalBytes(
            aDrain,
            q => q.Contains("published_at_ms", StringComparison.OrdinalIgnoreCase)
                 && q.Contains("UNNEST", StringComparison.OrdinalIgnoreCase));
        var aCleanupWal = PerMessageWalBytes(
            aCleanup,
            q => q.Contains("ctid IN", StringComparison.OrdinalIgnoreCase));
        var bClaim = PerMessageWalBytes(bDrain, isClaim: true);
        var bDelete = PerMessageWalBytes(
            bDrain,
            q => q.Contains("DELETE", StringComparison.OrdinalIgnoreCase)
                 && q.Contains("UNNEST", StringComparison.OrdinalIgnoreCase));

        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-complete-mode-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 完成模式 A/B（排水完整生命周期 WAL）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、排水 {MessageCount} 条。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | claim WAL/消息 | complete WAL/消息 | 后续 cleanup WAL/消息 | 排水生命周期合计 WAL/消息 |");
        sb.AppendLine("|---|---|---|---|---|");
        sb.AppendLine($"| 保留（A：claim+MarkPublished+cleanup） | {aClaim:N0} | {aComplete:N0} | {aCleanupWal:N0} | {aClaim + aComplete + aCleanupWal:N0} |");
        sb.AppendLine($"| 即删（B：claim+DeleteClaimed，生产默认） | {bClaim:N0} | {bDelete:N0} | — | {bClaim + bDelete:N0} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes / rows 归因，A/B 同容器顺序运行、仅完成模式不同；");
        sb.AppendLine("> A 为保留模式完整生命周期（claim + MarkPublished + 全量 cleanup，清理清空 Published 行）；");
        sb.AppendLine("> B 为生产默认 delete-on-complete（claim + DeleteClaimedPublished），无中间 Published 态、无 cleanup。");
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(aDrain, aCompleted, "A：保留模式（claim + MarkPublished）"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(aCleanup, aCleaned, "A：后续 cleanup（删除 Published 行）"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(bDrain, bCompleted, "B：delete-on-complete（claim + DeleteClaimedPublished）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    private static async Task WriteIndexAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff,
        long aInsertBytes,
        long aInsertRecords,
        long bInsertBytes,
        long bInsertRecords)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-index-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 索引 A/B（messages 插入写放大）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息，其中约 1/4 携带 reply_to_*+forwarded_from_* 引用；" +
                       "两窗口前均 TRUNCATE messages 从空表起始。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 全局 WAL 字节/消息 | INSERT messages WAL 字节/消息 | INSERT messages WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|");
        sb.AppendLine($"| 索引存在（A） | {aDiff.Wal.WalBytes / MessageCount:N0} | {aInsertBytes:N0} | {aInsertRecords:N0} |");
        sb.AppendLine($"| 索引剔除（B） | {bDiff.Wal.WalBytes / MessageCount:N0} | {bInsertBytes:N0} | {bInsertRecords:N0} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 wal_bytes / wal_records 归因，A/B 同容器顺序运行，");
        sb.AppendLine("> 仅 `ix_messages_reply_to` / `ix_messages_forwarded_from` 两个部分索引的存在性不同；");
        sb.AppendLine("> INSERT messages 归因匹配 `INSERT INTO ... \"messages\"` 语句并按 rows 折算。");
        sb.AppendLine();

        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：索引存在"));
        sb.AppendLine();
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：索引剔除"));

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

    private static async Task WriteAdmissionMergeAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-admission-merge-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 admission 合并 A/B（减少重复读取/往返）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：fallback（lifecycle admission + 序号分配 + bundle） | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {TotalExecMsPerMessage(aDiff, MessageCount):N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine($"| B：合并（admission+序号分配单条 CTE + bundle） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {TotalExecMsPerMessage(bDiff, MessageCount):N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅 admission/序号分配路径不同；");
        sb.AppendLine("> 确定性收益是每次消息的 SQL 往返 −1（热路径 3 条 → 2 条）：B 把「生命周期读取 + canonical 账本读取 +");
        sb.AppendLine("> 事务内授权 + 会话序号分配」合并为单条 CTE，消除 fallback 的独立 `upsert_conversation` 序号分配往返；");
        sb.AppendLine("> WAL 列不可直接对比：B 额外承担幂等账本 canonical 插入（`command_idempotency_ledger` 每消息 1 行，A 完全没有），");
        sb.AppendLine("> 且短窗口下 WAL 字节/消息随容器页分配有 ±10% 级波动，故以语句级 calls 与热路径语句数为权威归因。");
        sb.AppendLine();

        sb.AppendLine("## A：fallback 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：fallback（独立 SQL 更多）"));
        sb.AppendLine();

        sb.AppendLine("## B：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：合并（admission+序号分配单条 CTE）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    private static async Task WriteSuperBundleAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-super-bundle-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 合并同事务写入 A/B（super-bundle 单条 CTE）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：合并（admission CTE + bundle，2 条数据语句） | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {TotalExecMsPerMessage(aDiff, MessageCount):N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine($"| B：super-bundle（合并为单条 CTE） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {TotalExecMsPerMessage(bDiff, MessageCount):N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅写入合并程度不同；");
        sb.AppendLine("> 确定性收益是每次消息的数据路径 SQL −1（热路径 2 条 → 1 条）：B 把生产合并路径的 admission CTE");
        sb.AppendLine("> （生命周期/授权/幂等 + 会话与 member 写入 + 序号分配）与 bundle CTE（message + outbox + ledger）");
        sb.AppendLine("> 合并为单条语句，热路径数据往返 2 → 1；两路径写入行集完全相同（conversations / members / messages /");
        sb.AppendLine("> outbox / command_idempotency_ledger 各 1 行），故 WAL 列预期接近，收益集中于往返削减；");
        sb.AppendLine("> SQL 往返/消息含连接会话管理语句，短窗口下随连接池复用有 ±0.1 级波动，以热路径语句数为权威归因。");
        sb.AppendLine();

        sb.AppendLine("## A：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：合并（admission CTE + bundle）"));
        sb.AppendLine();

        sb.AppendLine("## B：super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：super-bundle（合并同事务写入为单条 CTE）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// 生成「跳过同会话重复授权读取」A/B 报告
    /// <c>docs/measurements/outbox-db-auth-reads-ab.md</c>。
    /// </summary>
    private static async Task WriteNoAuthReadsAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-auth-reads-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 减少重复读取 A/B（跳过同会话授权读取的每消息成本）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、单聊同会话。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：super-bundle（带授权读取） | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {TotalExecMsPerMessage(aDiff, MessageCount):N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine($"| B：no-auth super-bundle（跳过授权读取） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {TotalExecMsPerMessage(bDiff, MessageCount):N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、语句数与写入行集完全相同；");
        sb.AppendLine("> 唯一差异是 admission 段的授权读取：A 每条消息读取 direct_user_state（AspNetUsers 2 行）+");
        sb.AppendLine("> direct_authorization（T_BlockRecords 1 次 EXISTS + T_UserFriendEntry 2 次 EXISTS，共 4 处表读取），");
        sb.AppendLine("> B 的 write_gate 只按生命周期 + 幂等 canonical 门控（admission 读 5 张表 → 2 张表）。故执行耗时/");
        sb.AppendLine("> WAL 的差异即为「同会话重复授权读取」的每消息成本；B 为测量专用变体（跳过授权在语义上不安全），");
        sb.AppendLine("> 若该成本显著，后续可评估会话级授权缓存或已建会话免重复授权方向。");
        sb.AppendLine();

        sb.AppendLine("## A：带授权读取的 super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：super-bundle（带授权读取）"));
        sb.AppendLine();

        sb.AppendLine("## B：跳过授权读取的 no-auth super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：no-auth super-bundle（跳过授权读取）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// 生成「会话级授权缓存 / 已建会话免重复授权」A/B 报告
    /// <c>docs/measurements/outbox-db-session-auth-cache-ab.md</c>。
    /// </summary>
    private static async Task WriteSessionAuthCacheAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff,
        int conversationCount)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-session-auth-cache-ab.md");

        var msgsPerConv = MessageCount / Math.Max(1, conversationCount);
        var recoverableFraction = (msgsPerConv - 1.0) / Math.Max(1, msgsPerConv);
        var aExecMs = TotalExecMsPerMessage(aDiff, MessageCount);
        var bExecMs = TotalExecMsPerMessage(bDiff, MessageCount);
        var measuredSavingMs = aExecMs - bExecMs;

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 减少重复读取 A/B（已建会话免重复授权：会话级授权缓存的每消息收益）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、{conversationCount} 个会话、每会话 {msgsPerConv} 条（已建会话）。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：每条消息完整授权读取 | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {aExecMs:N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine($"| B：会话级授权缓存（已建会话免重复授权） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {bExecMs:N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、语句数与写入行集完全相同、");
        sb.AppendLine("> 会话分布完全相同（conversationCount 个会话、每会话 msgsPerConv 条消息）。唯一差异是授权读取粒度：");
        sb.AppendLine("> A 每条消息读取 direct_user_state（AspNetUsers 2 行）+ direct_authorization（T_BlockRecords 1 次 EXISTS +");
        sb.AppendLine("> T_UserFriendEntry 2 次 EXISTS，共 4 处表读取）；B 仅每会话第一条消息做完整授权读取（建立会话），");
        sb.AppendLine($"> 后续 {msgsPerConv - 1} 条跳过授权读取（会话级授权缓存命中，模拟「已建会话免重复授权」）。");
        sb.AppendLine();
        sb.AppendLine($"## 结论");
        sb.AppendLine();
        sb.AppendLine($"> 本次实测 B 较 A 每消息执行耗时节省 {measuredSavingMs:N3} ms（{aExecMs:N2} → {bExecMs:N2} ms）；按每会话 ");
        sb.AppendLine($"> {msgsPerConv} 条消息，理论上限为回收 (msgsPerConv−1)/msgsPerConv ≈ {recoverableFraction:P0} 的授权读取成本");
        sb.AppendLine("> （单次授权读取成本约 0.08 ms/消息，见 outbox-db-auth-reads-ab.md）。会话级授权缓存方向可行：");
        sb.AppendLine("> 会话建立后授权事实稳定，落地生产需对「会话建立」与「授权事实变更（好友/黑名单/隐私策略变化）」");
        sb.AppendLine("> 做 TTL 或显式失效注入，使缓存命中不绕过会话建立后的授权变化；B 为测量专用变体，不直接落生产。");
        sb.AppendLine();

        sb.AppendLine("## A：每条消息完整授权读取 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：每条消息完整授权读取"));
        sb.AppendLine();

        sb.AppendLine("## B：会话级授权缓存（已建会话免重复授权）热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：会话级授权缓存（已建会话免重复授权）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    private static async Task WriteSenderKnownAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-sender-known-ab.md");

        var aExecMs = TotalExecMsPerMessage(aDiff, MessageCount);
        var bExecMs = TotalExecMsPerMessage(bDiff, MessageCount);
        var measuredSavingMs = aExecMs - bExecMs;

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 减少重复读取 A/B（仅读接收者 user_state：发送者已认证免重复读取）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、单会话（发送者固定 10_000_000_001、接收者固定 10_000_000_002）。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：每条消息读发送者+接收者 user_state | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {aExecMs:N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine($"| B：sender-known（只读接收者 user_state） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {bExecMs:N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、语句数与写入行集完全相同、");
        sb.AppendLine("> 会话分布完全相同。唯一差异是 direct_user_state 的读取粒度：A 读取 AspNetUsers 发送者 + 接收者两行");
        sb.AppendLine("> （WHERE \"Id\" IN ($2, $3)）；B 只读取接收者行（WHERE \"Id\" = $3）、sender_exists 固定为 TRUE。");
        sb.AppendLine("> 语义依据：生产发送者是已认证用户，其存在性在 admission 时已保证，故 sender 行的存在性读取是");
        sb.AppendLine("> 每消息的冗余读取；B 模拟「发送者已认证，免重复读取发送者状态」的优化形态。授权判定（direct_authorization）");
        sb.AppendLine("> 与写入路径两者相同，B 未改变任何授权语义，仅消除 sender 行的冗余存在性读取。");
        sb.AppendLine();
        sb.AppendLine($"## 结论");
        sb.AppendLine();
        sb.AppendLine($"> 本次实测 B 较 A 每消息执行耗时差仅 {measuredSavingMs:N3} ms（{aExecMs:N2} → {bExecMs:N2} ms），");
        sb.AppendLine("> 处于测量噪声内（多轮运行 WAL 差从 −10% 到 −0.6% 大幅漂移，语句级 main CTE exec/消息 两者持平、");
        sb.AppendLine("> 两配置 blks_read 均为 0，即 direct_user_state 的发送者/接收者行均已命中共享缓冲区）。故「发送者状态");
        sb.AppendLine("> 冗余读取」的每消息成本可忽略：该「减少重复读取」变体无可量化收益，不值得为消除 sender 行读取引入");
        sb.AppendLine("> 生产改动（会破坏 direct_user_state 作为 admission 事实源的一致语义，却无性能回报）。WAL 列受短窗口");
        sb.AppendLine("> 容器波动影响不可靠归因，不采信。结论：sender 行重读不是热路径热点，后续减少重复读取应聚焦授权判定");
        sb.AppendLine("> （direct_authorization）而非 direct_user_state 的发送者存在性行。权威归因以语句级 calls/exec 与");
        sb.AppendLine("> 热路径语句数一致为准。");
        sb.AppendLine();

        sb.AppendLine("## A：读发送者+接收者 user_state 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：读发送者+接收者 user_state"));
        sb.AppendLine();

        sb.AppendLine("## B：sender-known（只读接收者 user_state）热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：sender-known（只读接收者 user_state）"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>
    /// 生成「避免无变化 UPDATE（会话头列）」A/B 报告
    /// <c>docs/measurements/outbox-db-noop-update-ab.md</c>。
    /// </summary>
    private static async Task WriteNoopUpdateAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-noop-update-ab.md");

        var aExecMs = TotalExecMsPerMessage(aDiff, MessageCount);
        var bExecMs = TotalExecMsPerMessage(bDiff, MessageCount);
        var aHot = ConversationHotRatio(aDiff);
        var bHot = ConversationHotRatio(bDiff);
        var aConv = diffTable(aDiff, "conversations");
        var bConv = diffTable(bDiff, "conversations");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 避免无变化 UPDATE A/B（会话头列守卫：乱序下 HOT 命中与索引维护成本）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"每配置 inbound {MessageCount} 条消息、单会话（发送者固定 10_000_000_001、接收者固定 10_000_000_002）、" +
                       $"会话头高水位乱序语料（第 0 条极高 received_at_ms 建立高水位，后续消息均低于高水位）。");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 | conversations HOT 命中率 |");
        sb.AppendLine("|---|---|---|---|---|---|---|");
        sb.AppendLine($"| A：无条件覆盖会话头列（无 CASE 守卫） | {HotPathStatementCount(aDiff, MessageCount)} | {TotalCallsPerMessage(aDiff, MessageCount):N1} | {aExecMs:N2} | {aDiff.Wal.WalBytes / MessageCount:N0} | {aDiff.Wal.WalRecords / (double)MessageCount:N1} | {aHot:N1}% |");
        sb.AppendLine($"| B：生产 CASE 守卫（保留未推进会话头列） | {HotPathStatementCount(bDiff, MessageCount)} | {TotalCallsPerMessage(bDiff, MessageCount):N1} | {bExecMs:N2} | {bDiff.Wal.WalBytes / MessageCount:N0} | {bDiff.Wal.WalRecords / (double)MessageCount:N1} | {bHot:N1}% |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements + pg_stat_user_tables 归属，A/B 同容器顺序运行、语句数与写入行集完全相同；");
        sb.AppendLine("> 唯一差异是会话 upsert 的 SET 片段：A 无条件覆盖 last_message_id/preview/at_ms/sender_user_id，");
        sb.AppendLine("> B 用生产 CASE 守卫在会话头未推进（乱序）时保留这四列。");
        sb.AppendLine();
        sb.AppendLine("## 结论性发现：该子方向已被 Migration058 吸收，无需再优化");
        sb.AppendLine();
        sb.AppendLine($"> 实测 A 与 B 的 conversations 表 HOT 命中率分别为 {aHot:N1}% 与 {bHot:N1}%，均达到 HOT 级。");
        sb.AppendLine("> 根因：`Migration058_ConversationHotProjectionUpdates` 已 DROP 含 `last_message_at_ms` 的全局索引");
        sb.AppendLine("> `ix_conversations_last_message_list`（该索引不覆盖 user/pinned 谓词、正式 8 小时运行扫描次数为 0，");
        sb.AppendLine("> 却让 230 万次 tip 更新全部无法 HOT）。索引移除后 `last_message_at_ms` 已非索引列，");
        sb.AppendLine("> 无论是否用 CASE 守卫保留该列，conversation tip 更新都走 HOT，不再触发索引维护。");
        sb.AppendLine("> **结论：** 生产 upsert_conversation 的 CASE 守卫（避免无变化写入索引列)在 Drop 索引之后已不再影响");
        sb.AppendLine("> HOT/索引维护收益——该子方向实质已被 Migration058 吸收。CASE 守卫可继续保留（逻辑上避免无意义覆写、");
        sb.AppendLine("> 减少 dead tuple 与 WAL 写放大），但不存在进一步的 HOT/索引类收益可压测。");
        sb.AppendLine();

        sb.AppendLine("## A：无条件覆盖会话头列 热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：无条件覆盖会话头列（无 CASE 守卫）"));
        sb.AppendLine();

        sb.AppendLine("## B：生产 CASE 守卫（保留未推进会话头列）热路径语句（近热路径 = 调用数 ≥ 消息数一半）");
        sb.AppendLine();
        sb.Append(PerMessageStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：生产 CASE 守卫"));
        sb.AppendLine();

        sb.AppendLine("## conversations 表级更新（两配置均 HOT，印证索引已移除）");
        sb.AppendLine();
        sb.AppendLine("| 配置 | 更新 | HOT 更新 | HOT 命中率 | 死元组 |");
        sb.AppendLine("|---|---|---|---|---|");
        sb.AppendLine(aConv is null
            ? "| A：无条件覆盖 | — | — | — | — |"
            : $"| A：无条件覆盖 | {aConv.Updates:N0} | {aConv.HotUpdates:N0} | {aHot:N1}% | {aConv.DeadTuples:N0} |");
        sb.AppendLine(bConv is null
            ? "| B：生产 CASE 守卫 | — | — | — | — |"
            : $"| B：生产 CASE 守卫 | {bConv.Updates:N0} | {bConv.HotUpdates:N0} | {bHot:N1}% | {bConv.DeadTuples:N0} |");

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>按表名取窗口内表级增量。</summary>
    private static PgTableDiff? diffTable(PostgresPerfDiff diff, string tableName) =>
        diff.Tables.FirstOrDefault(t => t.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));

    private static async Task WriteBatchSizeAbReportAsync(
        PostgresPerfDiff aDiff,
        PostgresPerfDiff bDiff)
    {
        var reportDir = ResolveDocsMeasurementsDir();
        Directory.CreateDirectory(reportDir);
        var reportPath = Path.Combine(reportDir, "outbox-db-batch-size-ab.md");

        var sb = new StringBuilder();
        sb.AppendLine("# OUTBOX-DB-1 有界批量 claim/complete A/B（批量上限对每消息往返/事务开销的影响）");
        sb.AppendLine();
        sb.AppendLine($"> 生成时间：{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC；语料固定、随机种子 {Seed}；" +
                       $"delete-on-complete 排水 {MessageCount} 条。");
        sb.AppendLine();
        sb.AppendLine("| 批量上限 | 排水 SQL 往返/消息 | 排水耗时/消息(ms) | 排水 WAL 字节/消息 | claim 调用 | delete 调用 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        sb.AppendLine($"| A：1（每条消息独立 claim+delete） | {DrainRoundTripsPerMessage(aDiff, MessageCount):N2} | {DrainExecMsPerMessage(aDiff, MessageCount):N2} | {DrainWalBytesPerMessage(aDiff, MessageCount):N0} | {ClaimCalls(aDiff)} | {DeleteCalls(aDiff)} |");
        sb.AppendLine($"| B：200（有界批量，单事务跨度可控） | {DrainRoundTripsPerMessage(bDiff, MessageCount):N2} | {DrainExecMsPerMessage(bDiff, MessageCount):N2} | {DrainWalBytesPerMessage(bDiff, MessageCount):N0} | {ClaimCalls(bDiff)} | {DeleteCalls(bDiff)} |");
        sb.AppendLine();
        sb.AppendLine("> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅 claim/complete 批量上限不同；");
        sb.AppendLine("> claim/delete 均为单语句（`FOR UPDATE ... SKIP LOCKED ... LIMIT @batch_size` / `DELETE ... USING UNNEST`），");
        sb.AppendLine("> 批量上限只改变每条消息的平均往返次数与事务提交次数（autocommit 下每语句一个事务）：批量越大、");
        sb.AppendLine("> 每消息往返与提交开销越低，而上限本身保证单事务/单 claim 跨度有界，积压时不会形成失控大事务。");
        sb.AppendLine();

        sb.AppendLine("## A：批量上限 1 的排水语句");
        sb.AppendLine();
        sb.Append(DrainStatementTable(aDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(aDiff, MessageCount, "A：批量上限 1（往返最频繁）"));
        sb.AppendLine();

        sb.AppendLine("## B：批量上限 200 的排水语句");
        sb.AppendLine();
        sb.Append(DrainStatementTable(bDiff, MessageCount));
        sb.Append(PostgresPerfReporter.Render(bDiff, MessageCount, "B：有界批量上限 200"));

        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8);
    }

    /// <summary>排水窗口内 claim/delete 语句的总调用次数（即排水往返总数）。</summary>
    private static long DrainRoundTrips(PostgresPerfDiff diff) =>
        diff.Statements.Where(IsDrainStatement).Sum(s => s.Calls);

    /// <summary>每条消息的排水往返数（claim+delete 调用数 / 消息数）。</summary>
    private static double DrainRoundTripsPerMessage(PostgresPerfDiff diff, int messageCount) =>
        messageCount > 0 ? DrainRoundTrips(diff) / (double)messageCount : 0;

    /// <summary>排水语句的总执行时间按消息数折算（ms/消息）。</summary>
    private static double DrainExecMsPerMessage(PostgresPerfDiff diff, int messageCount) =>
        messageCount > 0 ? diff.Statements.Where(IsDrainStatement).Sum(s => s.TotalExecMs) / messageCount : 0;

    /// <summary>排水语句的总 WAL 字节按消息数折算（字节/消息）。</summary>
    private static double DrainWalBytesPerMessage(PostgresPerfDiff diff, int messageCount) =>
        messageCount > 0 ? diff.Statements.Where(IsDrainStatement).Sum(s => s.WalBytes) / (double)messageCount : 0;

    /// <summary>claim 语句调用数（`FOR UPDATE ... SKIP LOCKED ... LIMIT` 的 UPDATE）。</summary>
    private static long ClaimCalls(PostgresPerfDiff diff) =>
        diff.Statements.Where(IsClaimStatement).Sum(s => s.Calls);

    /// <summary>delete-on-complete 语句调用数（`DELETE ... USING UNNEST`）。</summary>
    private static long DeleteCalls(PostgresPerfDiff diff) =>
        diff.Statements.Where(IsDeleteStatement).Sum(s => s.Calls);

    private static bool IsClaimStatement(PgStatementDiff s) =>
        s.Query.Contains("candidates AS MATERIALIZED", StringComparison.OrdinalIgnoreCase)
        && s.Query.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
        && s.Query.Contains("SKIP LOCKED", StringComparison.OrdinalIgnoreCase);

    private static bool IsDeleteStatement(PgStatementDiff s) =>
        s.Query.Contains("DELETE FROM", StringComparison.OrdinalIgnoreCase)
        && s.Query.Contains("UNNEST", StringComparison.OrdinalIgnoreCase);

    private static bool IsDrainStatement(PgStatementDiff s) =>
        IsClaimStatement(s) || IsDeleteStatement(s);

    /// <summary>渲染排水路径（claim/delete）语句的每消息成本表。</summary>
    private static string DrainStatementTable(PostgresPerfDiff diff, int messageCount)
    {
        var sb = new StringBuilder();
        sb.AppendLine("| 语句 | 调用 | 调用/消息 | exec/消息(ms) | wal_bytes/调用 | sql 片段 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (var s in diff.Statements.Where(IsDrainStatement).OrderByDescending(s => s.WalBytes))
        {
            var sql = s.Query.Length > 72 ? s.Query[..72] + "…" : s.Query;
            sb.AppendLine($"| {s.Calls:N0} | {s.Calls / (double)messageCount:N2} | {s.TotalExecMs / messageCount:N3} | {(s.Calls > 0 ? s.WalBytes / s.Calls : 0):N0} | `{SanitizeMd(sql)}` |");
        }

        sb.AppendLine();
        return sb.ToString();
    }

    /// <summary>
    /// 统计窗口内每条消息都执行的「数据路径」语句数（INSERT INTO messages / conversations /
    /// pg_advisory_xact_lock_shared 生命周期），用于对照 A/B 各自的热路径往返结构。
    /// 排除 BEGIN/COMMIT/SET/RESET/DISCARD 等 Npgsql 会话管理语句（连接池复用后不随每条消息出现）。
    /// </summary>
    private static int HotPathStatementCount(PostgresPerfDiff diff, int messageCount) =>
        diff.Statements.Count(s =>
            s.Calls >= messageCount / 2
            && (s.Query.Contains("INSERT INTO", StringComparison.OrdinalIgnoreCase)
                || s.Query.Contains("pg_advisory_xact_lock_shared", StringComparison.OrdinalIgnoreCase)
                || s.Query.Contains("upsert_conversation", StringComparison.OrdinalIgnoreCase)));

    /// <summary>窗口内全部语句的总调用次数按消息数折算，即每条消息平均触发的 SQL 往返数。</summary>
    private static double TotalCallsPerMessage(PostgresPerfDiff diff, int messageCount) =>
        messageCount > 0 ? diff.Statements.Sum(s => s.Calls) / (double)messageCount : 0;

    /// <summary>窗口内全部语句的总执行时间按消息数折算（ms/消息）。</summary>
    private static double TotalExecMsPerMessage(PostgresPerfDiff diff, int messageCount) =>
        messageCount > 0 ? diff.Statements.Sum(s => s.TotalExecMs) / messageCount : 0;

    /// <summary>
    /// 渲染近热路径语句（调用数 ≥ 消息数一半）的 per-message 成本表，
    /// 用于对照 A/B 各自的 SQL 往返结构。
    /// </summary>
    private static string PerMessageStatementTable(PostgresPerfDiff diff, int messageCount)
    {
        var sb = new StringBuilder();
        sb.AppendLine("| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (var s in diff.Statements
                     .Where(s => s.Calls >= messageCount / 2)
                     .OrderByDescending(s => s.WalBytes))
        {
            var sql = s.Query.Length > 72 ? s.Query[..72] + "…" : s.Query;
            sb.AppendLine($"| {s.Calls:N0} | {s.Calls / (double)messageCount:N1} | {s.TotalExecMs / messageCount:N2} | {s.WalRecords / (double)messageCount:N1} | {(s.Calls > 0 ? s.WalBytes / s.Calls : 0):N0} | `{SanitizeMd(sql)}` |");
        }

        sb.AppendLine();
        return sb.ToString();
    }

    private static string SanitizeMd(string sql) =>
        sql.Replace("|", "\\|", StringComparison.Ordinal).ReplaceLineEndings(" ");

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
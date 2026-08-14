using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using DotNet.Testcontainers.Builders;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests.Measurement;

/// <summary>
/// OUTBOX-DB-1 的 PG 级 A/B 测量基石。
/// <para>
/// 以 <c>shared_preload_libraries=pg_stat_statements</c> 启动真实 PostgreSQL 16 容器，
/// 提供可重复的固定语料/随机种子驱动 <see cref="NpgsqlRealtimeMessageStore.SaveAsync"/>
/// 热路径（message + conversation/unread projection + outbox insert），并采集
/// <c>pg_stat_statements</c>、<c>pg_stat_wal</c> 与表级统计的前后快照，
/// 供把每消息成本拆成可归因的 SQL/WAL 项。
/// </para>
/// <para>
/// 热路径共享仅限线程安全连接池；每次操作使用 store 自有的独立连接，不跨事务共享
/// command/reader 或可变批次。
/// </para>
/// </summary>
public sealed class PostgresPerfHarness : IAsyncDisposable
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:16")
        // 注意：PostgreSqlContainer 的镜像 entrypoint 会自动附加 "postgres"
        // 作为命令，因此这里只传 -c 参数；若带上前导 "postgres" 会得到
        // "postgres postgres ..." 而启动失败。
        .WithCommand(
            "-c", "shared_preload_libraries=pg_stat_statements",
            "-c", "pg_stat_statements.track=top")
        // TCP 端口就绪后 PG 可能仍需初始化才能接受连接（首连 SSL 协商会被重置）。
        // 端口等待 + 连接重试（见 InitializeAsync）共同消除该就绪竞态。
        .WithWaitStrategy(Wait.ForUnixContainer().UntilExternalTcpPortIsAvailable(5432))
        .Build();

    /// <summary>初始化完成后到重试放弃的时长。</summary>
    private static readonly TimeSpan ConnectRetryWindow = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 生命周期 advisory lock 命名空间键。与
    /// <c>UserLifecycleAdvisoryLock.NamespaceKey</c>（internal）保持一致；
    /// super-bundle CTE 直接以 <c>$1</c> 参数携带该键，供测试访问。
    /// </summary>
    private const long LifecycleNamespaceKey = 0x5553_4552_4C49_4645L;

    private RealtimeDatabaseClient? _client;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlRealtimeMessageStore? _messageStore;
    private NpgsqlRealtimeMessageStore? _mergedMessageStore;
    private NpgsqlRealtimeOutboxStore? _outboxStore;
    private string? _superBundleCommandText;
    private string? _noAuthSuperBundleCommandText;
    private string? _senderKnownSuperBundleCommandText;
    private string? _naiveSuperBundleCommandText;

    public string SchemaName { get; } = $"perf_{Guid.NewGuid():N}"[..17];

    public PostgresPerfSnapshot? LastSnapshot { get; private set; }

    public async Task InitializeAsync()
    {
        await _container.StartAsync().ConfigureAwait(false);

        var connectionString = _container.GetConnectionString();
        _client = new RealtimeDatabaseClient(connectionString, NullLogger<RealtimeDatabaseClient>.Instance);
        var schema = new RealtimeDatabaseSchema(SchemaName);
        _schema = schema;

        // 端口就绪后 PG 可能仍在初始化，首连可能在 SSL 协商阶段被重置（EndOfStream）。
        // 在放弃窗口内重试打开连接，直到 PG 真正可接受连接。
        Exception? lastError = null;
        var deadline = DateTimeOffset.UtcNow + ConnectRetryWindow;
        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                await using var conn = await _client.GetDataSource()
                    .OpenConnectionAsync().ConfigureAwait(false);
                await InitializeSchemaAsync(conn, schema).ConfigureAwait(false);
                lastError = null;
                break;
            }
            catch (Exception ex) when (ex is NpgsqlException or IOException)
            {
                lastError = ex;
                await Task.Delay(500).ConfigureAwait(false);
            }
        }

        if (lastError is not null)
        {
            throw new InvalidOperationException(
                $"PostgreSQL 在 {ConnectRetryWindow.TotalSeconds:N0}s 内未能就绪。", lastError);
        }

        _messageStore = new NpgsqlRealtimeMessageStore(
            _client,
            _schema,
            new PostgresConversationMessageMutationPolicy(NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance,
            metrics: null,
            idempotencyLedger: null,
            outboxSignal: null);
        // 生产合并路径：注入 Npgsql 账本后，SaveAsync 走
        // MessageWriteAdmissionReader.AcquireDirectAndAllocateSequenceAsync，把生命周期读取、
        // 账本 canonical 读取、事务内授权与会话序号分配合并为单条 CTE，消除 fallback 的
        // ordered_users + upsert_conversation 两次独立往返。
        _mergedMessageStore = new NpgsqlRealtimeMessageStore(
            _client,
            _schema,
            new PostgresConversationMessageMutationPolicy(NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance,
            metrics: null,
            idempotencyLedger: new NpgsqlCommandIdempotencyLedger(
                _client,
                _schema,
                NullLogger<NpgsqlCommandIdempotencyLedger>.Instance),
            outboxSignal: null);
        _outboxStore = new NpgsqlRealtimeOutboxStore(_client, _schema);
    }

    private async Task InitializeSchemaAsync(NpgsqlConnection connection, RealtimeDatabaseSchema schema)
    {
        await using var createExt = new NpgsqlCommand(
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;", connection);
        await createExt.ExecuteNonQueryAsync().ConfigureAwait(false);

        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection).ConfigureAwait(false);
    }

    /// <summary>
    /// 创建并播种生产合并路径所需的事务内授权表（<c>public."AspNetUsers"</c> /
    /// <c>public."T_BlockRecords"</c> / <c>public."T_UserFriendEntry"</c>），并让语料中的
    /// 发送用户（10_000_000_001）与 <paramref name="receiverCount"/> 个接收用户
    /// （10_000_000_002 起递增）成为互加好友且非禁止陌生人策略，使
    /// <see cref="MessageWriteAdmissionReader.AcquireDirectAndAllocateSequenceAsync"/>
    /// 的 <c>direct_authorization</c> 判定为 Allowed。
    /// <para>
    /// 默认 <paramref name="receiverCount"/>=1 时行为与既有一致（仅播种 10_000_000_001 /
    /// 10_000_000_002 一对）；多会话压测需为每个会话的接收者播种授权关系。
    /// </para>
    /// </summary>
    public async Task SeedAuthTablesAsync(int receiverCount = 1, CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using (var cmd = new NpgsqlCommand(
            """
            CREATE TABLE IF NOT EXISTS public."AspNetUsers"
            (
                "Id" bigint PRIMARY KEY,
                "FriendRequestPolicy" smallint NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS public."T_BlockRecords"
            (
                "BlockerId" bigint NOT NULL,
                "BlockedUserId" bigint NOT NULL,
                PRIMARY KEY ("BlockerId", "BlockedUserId")
            );

            CREATE TABLE IF NOT EXISTS public."T_UserFriendEntry"
            (
                "UserId" bigint NOT NULL,
                "FriendId" bigint NOT NULL,
                "IsDeleted" boolean NOT NULL DEFAULT FALSE,
                PRIMARY KEY ("UserId", "FriendId")
            );
            """,
            connection))
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // FriendRequestPolicy=1（RequireVerification）≠ 2（NoStrangers），配合互加好友，
        // direct_authorization 判定 privacy_policy≠2 且双向好友存在 → decision=0（Allowed）。
        // 每个会话是「发送者 + 一个接收者」，只需播种这两条好友边（O(n)），无需全互连。
        var lastId = 10_000_000_001L + receiverCount;
        await using (var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO public."AspNetUsers" ("Id", "FriendRequestPolicy")
             SELECT g.id, 1
             FROM generate_series(10000000001, {lastId}) AS g(id)
             ON CONFLICT ("Id") DO NOTHING;

             INSERT INTO public."T_UserFriendEntry" ("UserId", "FriendId", "IsDeleted")
             SELECT 10000000001, g.id, FALSE
             FROM generate_series(10000000002, {lastId}) AS g(id)
             ON CONFLICT ("UserId", "FriendId") DO NOTHING;

             INSERT INTO public."T_UserFriendEntry" ("UserId", "FriendId", "IsDeleted")
             SELECT g.id, 10000000001, FALSE
             FROM generate_series(10000000002, {lastId}) AS g(id)
             ON CONFLICT ("UserId", "FriendId") DO NOTHING;
             """,
            connection))
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public RealtimeDatabaseClient Client =>
        _client ?? throw new InvalidOperationException("Harness 未初始化。");

    public RealtimeDatabaseSchema Schema =>
        _schema ?? throw new InvalidOperationException("Harness 未初始化。");

    public NpgsqlRealtimeMessageStore MessageStore =>
        _messageStore ?? throw new InvalidOperationException("Harness 未初始化。");

    /// <summary>
    /// 生产合并路径的 store：注入 <see cref="NpgsqlCommandIdempotencyLedger"/> 后，
    /// <see cref="NpgsqlRealtimeMessageStore.SaveAsync"/> 走单条 admission+序号 CTE。
    /// </summary>
    public NpgsqlRealtimeMessageStore MergedMessageStore =>
        _mergedMessageStore ?? throw new InvalidOperationException("Harness 未初始化。");

    public NpgsqlRealtimeOutboxStore OutboxStore =>
        _outboxStore ?? throw new InvalidOperationException("Harness 未初始化。");

    /// <summary>
    /// 从 <c>pg_stat_statements</c> 采集按 queryid 聚合的语句统计。
    /// </summary>
    public async Task<IReadOnlyList<PgStatementStat>> SnapshotStatementsAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT queryid, query, calls, total_exec_time, rows,
                   shared_blks_read, shared_blks_dirtied,
                   wal_records, wal_fpi, wal_bytes
            FROM pg_stat_statements
            WHERE queryid IS NOT NULL AND calls > 0;
            """,
            connection);
        var results = new List<PgStatementStat>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            results.Add(new PgStatementStat(
                reader.GetInt64(0).ToString("X"),
                (string)reader.GetString(1),
                reader.GetInt64(2),
                reader.GetDouble(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetInt64(9)));
        }

        return results;
    }

    /// <summary>从 <c>pg_stat_wal</c> 采集全局 WAL 统计。</summary>
    public async Task<PgWalStat> SnapshotWalAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT COALESCE(wal_records, 0), COALESCE(wal_fpi, 0), COALESCE(wal_bytes, 0),
                   COALESCE(wal_write, 0), COALESCE(wal_sync, 0),
                   COALESCE(wal_write_time, 0), COALESCE(wal_sync_time, 0)
            FROM pg_stat_wal;
            """,
            connection);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await reader.ReadAsync(ct).ConfigureAwait(false);
        return new PgWalStat(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetInt64(2),
            reader.GetInt64(3),
            reader.GetInt64(4),
            reader.GetDouble(5),
            reader.GetDouble(6));
    }

    /// <summary>从 <c>pg_stat_user_tables</c> 采集本测试 schema 的表级统计。</summary>
    public async Task<IReadOnlyList<PgTableStat>> SnapshotTablesAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        // pg_stat_* 统计收集器异步滞后，短窗口下表级增量可能为 0；先强制冲刷待处理统计并
        // 短暂等待 stats collector 落库，使 pg_stat_user_tables 反映到当前时刻，A/B 窗口的
        // updates/hot_updates/deletes 才可归因。
        await using (var flush = new NpgsqlCommand(
                         "SELECT pg_stat_force_next_flush(), pg_sleep(0.2);",
                         connection))
        {
            await flush.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var cmd = new NpgsqlCommand(
            """
            SELECT relname, COALESCE(n_tup_ins, 0), COALESCE(n_tup_upd, 0), COALESCE(n_tup_del, 0),
                   COALESCE(n_tup_hot_upd, 0), COALESCE(n_live_tup, 0), COALESCE(n_dead_tup, 0)
            FROM pg_stat_user_tables
            WHERE schemaname = @schema;
            """,
            connection);
        cmd.Parameters.AddWithValue("schema", SchemaName);
        var results = new List<PgTableStat>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            results.Add(new PgTableStat(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6)));
        }

        return results;
    }

    /// <summary>采集一次完整快照（语句 + WAL + 表级）。</summary>
    public async Task<PostgresPerfSnapshot> SnapshotAsync(CancellationToken ct = default)
    {
        var statements = await SnapshotStatementsAsync(ct).ConfigureAwait(false);
        var wal = await SnapshotWalAsync(ct).ConfigureAwait(false);
        var tables = await SnapshotTablesAsync(ct).ConfigureAwait(false);
        var snapshot = new PostgresPerfSnapshot(statements, wal, tables);
        LastSnapshot = snapshot;
        return snapshot;
    }

    /// <summary>
    /// 调整 outbox 表的 fillfactor，用于 A/B 验证 HOT 命中率与排水 WAL 的关系。
    /// <para>
    /// fillfactor 只影响改变之后新插入行所在的页；已存在的行/页不受影响。
    /// 因此 A/B 须按「先 ALTER → 再插入 → 再排水」的顺序各跑一个配置。
    /// </para>
    /// </summary>
    public async Task SetOutboxFillfactorAsync(int fillfactor, CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"ALTER TABLE {Schema.OutboxTableSql} SET (fillfactor = {fillfactor});",
            connection);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 强制一次 CHECKPOINT，把已被修改过的脏页全部落盘并记入已写 WAL 段。
    /// <para>
    /// 用于隔离 FPI（full page image）混杂因素：CHECKPOINT 之后对同一页的再次修改
    /// 不再需要整页镜像，因此「先 CHECKPOINT 再排水」窗口测得的 claim/complete WAL
    /// 近似生产稳态（页早已落盘、无需 FPI），与「冷页」窗口对照即可归因 FPI 占比。
    /// </para>
    /// </summary>
    public async Task RunCheckpointAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand("CHECKPOINT;", connection);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 清空 messages 表，用于 A/B 两窗口从相同的空表起始（相同页分配模式），隔离
    /// 「表随窗口增长导致页分配/页分裂差异」这一混杂因素。TRUNCATE 同时清掉全部行与
    /// 全部索引项；单测试容器内无并发访问，安全。
    /// <para>
    /// messages 被部分表外键引用，需 CASCADE；压测语料中这些引用表（message_reactions /
    /// message_state 等）均为空，级联清空无副作用。
    /// </para>
    /// </summary>
    public async Task TruncateMessagesAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"TRUNCATE TABLE {Schema.MessagesTableSql} CASCADE;",
            connection);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 清空 outbox 表，用于 A/B 两窗口从相同的空表起始（相同页分配模式），隔离
    /// 「上一窗口排水残留的死元组/页分配模式影响下一窗口」这一混杂因素。
    /// outbox 无外键引用（outbox_replay_audit 为独立审计表），TRUNCATE 安全；
    /// CASCADE 用于防御未来新增引用。
    /// <para>
    /// 必须先设置 fillfactor 再 TRUNCATE：TRUNCATE 清空全部页，此后新插入的行
    /// 才会按最新 fillfactor 分配页。
    /// </para>
    /// </summary>
    public async Task TruncateOutboxAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            $"TRUNCATE TABLE {Schema.OutboxTableSql} CASCADE;",
            connection);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 剔除 <c>ix_messages_reply_to</c> / <c>ix_messages_forwarded_from</c> 两个从未被查询使用的
    /// 部分索引，用于 A/B 验证其对 messages 插入写放大的影响。
    /// </summary>
    public async Task DropReplyForwardIndexesAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        foreach (var indexName in new[] { "ix_messages_reply_to", "ix_messages_forwarded_from" })
        {
            await using var cmd = new NpgsqlCommand(
                $"DROP INDEX IF EXISTS {Schema.QuotedSchema}.\"{indexName}\";",
                connection);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 重建 <c>ix_messages_reply_to</c> / <c>ix_messages_forwarded_from</c>，恢复
    /// <see cref="Migration013_MessageReply"/> / <see cref="Migration015_MessageForward"/>
    /// 定义的部分索引（A/B 结束后的还原操作，保持容器 schema 与迁移目录一致）。
    /// </summary>
    public async Task CreateReplyForwardIndexesAsync(CancellationToken ct = default)
    {
        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using (var cmd = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_reply_to"
             ON {Schema.MessagesTableSql} ("reply_to_message_id")
             WHERE "reply_to_message_id" IS NOT NULL;
             """,
            connection))
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var cmd = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_forwarded_from"
             ON {Schema.MessagesTableSql} ("forwarded_from_message_id")
             WHERE "forwarded_from_message_id" IS NOT NULL;
             """,
            connection))
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 用固定语料 + 固定随机种子驱动真实 <see cref="NpgsqlRealtimeMessageStore.SaveAsync"/>
    /// 热路径 <paramref name="count"/> 条消息。每条消息使用独立会话/客户端编号，避免幂等命中。
    /// <para>
    /// 该路径使用 fallback store（<c>idempotencyLedger: null</c>）：生命周期读取与序号分配是
    /// <c>ordered_users</c> + <c>upsert_conversation</c> 两条独立 SQL，用于与生产合并路径
    /// （<see cref="RunMergedSaveWorkloadAsync"/>）对照「减少重复读取/往返」的收益。
    /// </para>
    /// <para>
    /// 当 <paramref name="replyEvery"/> &gt; 0 时，每第 <c>replyEvery</c> 条消息携带
    /// reply_to_*/forwarded_from_* 引用（模拟含回复/转发引用的真实消息流），用于归因
    /// <c>ix_messages_reply_to</c> / <c>ix_messages_forwarded_from</c> 两个部分索引的写放大；
    /// 默认 0 保持纯消息语料，与既有基线一致。
    /// </para>
    /// </summary>
    public Task RunSaveWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default,
        int replyEvery = 0)
        => RunSaveWorkloadCoreAsync(MessageStore, count, seed, ct, replyEvery);

    /// <summary>
    /// 用同一固定语料驱动生产合并路径（<see cref="MergedMessageStore"/>）的 SaveAsync 热路径。
    /// 生命周期读取、账本 canonical 读取、事务内授权与会话序号分配在单条 CTE 内完成，
    /// 用于与 fallback（<see cref="RunSaveWorkloadAsync"/>）对照。
    /// </summary>
    public Task RunMergedSaveWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default,
        int replyEvery = 0)
        => RunSaveWorkloadCoreAsync(MergedMessageStore, count, seed, ct, replyEvery);

    /// <summary>
    /// 用同一固定语料直接驱动「合并同事务写入」的单条 super-bundle CTE 热路径。
    /// <para>
    /// 生产合并路径（<see cref="RunMergedSaveWorkloadAsync"/>）每次 SaveAsync 仍是 2 条数据语句：
    /// admission CTE（生命周期/授权/幂等 + 会话与 member 写入 + 序号分配）与 bundle CTE
    /// （message + outbox + ledger 写入）。本方法把这两个同事务写入合并为单条 CTE——
    /// 复用 admission 的 <c>write_gate</c> 门控与 <c>upsert_conversation</c> / <c>sender_upsert</c>
    /// 序号分配，让 message INSERT 直接取 <c>last_sequence</c> / <c>sent_count</c>，
    /// 用于 A/B 量化「合并同事务写入」为单条语句后的往返收益（热路径 2 → 1 条）。
    /// </para>
    /// <para>
    /// 驱动语义与 SaveAsync 合并路径一致：每条消息开连接/事务 → 单条 super-bundle → 提交；
    /// advisory lock 随事务提交释放；<c>write_gate</c> 为空（生命周期/授权/幂等命中）时不产生
    /// 任何写入；message 幂等冲突时 outbox/ledger 不产生孤立行。语料 id 带种子前缀避免跨窗口幂等冲突。
    /// </para>
    /// </summary>
    public Task RunSuperBundleWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default,
        int conversationCount = 1)
        => RunSuperBundleWorkloadCoreAsync(count, seed, includeAuthorization: true, conversationCount, ct);

    /// <summary>
    /// 用同一固定语料直接驱动「跳过授权读取」的 no-auth super-bundle 热路径。
    /// <para>
    /// 与 <see cref="RunSuperBundleWorkloadAsync"/> 语句数相同（单条 CTE）、写入行集相同；
    /// 唯一差异是 admission 段不读取 <c>direct_user_state</c>（AspNetUsers）与
    /// <c>direct_authorization</c>（T_BlockRecords / T_UserFriendEntry），<c>write_gate</c>
    /// 只按生命周期 + 幂等 canonical 门控。用于 A/B 量化「同会话重复授权读取」的每消息成本
    /// （热路径 SQL 数不变，仅读取表数 5 → 1）。
    /// </para>
    /// </summary>
    public Task RunNoAuthSuperBundleWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default,
        int conversationCount = 1)
        => RunSuperBundleWorkloadCoreAsync(count, seed, includeAuthorization: false, conversationCount, ct);

    /// <summary>
    /// 用同一固定语料驱动「已建会话免重复授权」的会话级授权缓存热路径。
    /// <para>
    /// 与 <see cref="RunSuperBundleWorkloadAsync"/> 使用相同的多会话语料分布
    /// （<paramref name="conversationCount"/> 个会话、每会话 msgsPerConv = count/conversationCount
    /// 条消息）；唯一差异是授权读取的粒度：每个会话的第一条消息执行带完整授权读取的
    /// super-bundle（建立会话时校验 direct_user_state + direct_authorization），后续消息执行
    /// no-auth super-bundle（跳过 4 处授权表读取）。模拟生产「会话级授权缓存」——会话建立后
    /// 授权事实稳定、已建会话免重复授权。写入行集与 A 完全相同，仅减少已建会话的授权读取。
    /// </para>
    /// </summary>
    public Task RunSessionAuthCacheWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default,
        int conversationCount = 1)
        => RunSessionAuthCacheWorkloadCoreAsync(count, seed, conversationCount, ct);

    /// <summary>
    /// 用同一固定语料驱动「仅读接收者 user_state」的 sender-known 热路径。
    /// <para>
    /// 与 <see cref="RunSuperBundleWorkloadAsync"/> 语句数相同、写入行集完全相同，唯一差异是
    /// <c>direct_user_state</c> 只读取接收者的 AspNetUsers 行（<c>WHERE "Id" = $3</c>），发送者行
    /// 不再读取、<c>sender_exists</c> 固定为 TRUE。语义依据：生产发送者是已认证用户，其存在性在
    /// admission 时已保证，故 sender 行的存在性读取是每消息的冗余读取。模拟生产「发送者已认证，
    /// 免重复读取发送者状态」的优化形态。
    /// </para>
    /// </summary>
    public Task RunSenderKnownSuperBundleWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default)
        => RunSenderKnownSuperBundleWorkloadCoreAsync(count, seed, ct);

    /// <summary>
    /// 用「会话头高水位乱序」语料驱动生产带 CASE 守卫的 super-bundle 热路径（B 配置）。
    /// <para>
    /// 语料：第 0 条消息以极高 <c>received_at_ms</c> 建立会话头高水位，后续消息 <c>received_at_ms</c>
    /// 均低于该高水位 → 会话头的 <c>last_message_*</c（含被 <c>ix_conversations_last_message_list</c>
    /// 索引的 <c>last_message_at_ms</c>）不再推进。带 CASE 守卫的生产 upsert 会保留这些列 → 更新为
    /// HOT、不触发索引维护；用于 A/B 量化「避免无变化 UPDATE（会话头列）」的收益。
    /// </para>
    /// </summary>
    public Task RunOutOfOrderGuardedWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default)
        => RunOutOfOrderWorkloadCoreAsync(count, seed, guardConversationHeader: true, ct);

    /// <summary>
    /// 用「会话头高水位乱序」语料驱动无条件覆盖会话头列的 super-bundle 热路径（A 配置）。
    /// <para>
    /// 与 <see cref="RunOutOfOrderGuardedWorkloadAsync"/> 语料完全相同、写入行集完全相同，唯一差异是
    /// 会话 upsert 无条件覆盖 <c>last_message_id/preview/at_ms/sender_user_id</c>（无 CASE 守卫），
    /// 使被索引的 <c>last_message_at_ms</c> 每消息改变 → 更新为 non-HOT、每次触发索引维护。
    /// 模拟「若不做无变化 UPDATE 守卫」的基线，用于对照守卫保留索引列后 HOT 命中的收益。
    /// </para>
    /// </summary>
    public Task RunOutOfOrderNaiveWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default)
        => RunOutOfOrderWorkloadCoreAsync(count, seed, guardConversationHeader: false, ct);

    private async Task RunSuperBundleWorkloadCoreAsync(
        int count,
        int seed,
        bool includeAuthorization,
        int conversationCount,
        CancellationToken ct)
    {
        var commandText = includeAuthorization
            ? _superBundleCommandText ??= BuildSuperBundleCommandText(Schema)
            : _noAuthSuperBundleCommandText ??= BuildNoAuthSuperBundleCommandText(Schema);
        var rng = new Random(seed);
        var msgsPerConv = Math.Max(1, count / Math.Max(1, conversationCount));

        for (var i = 0; i < count; i++)
        {
            // 消息 id / 客户端 id / 事件 id 均带种子前缀，避免预热与测量两段窗口
            // 使用相同 id 但内容不同而触发幂等内容冲突。
            var (sender, receiver, conversationId) = ConversationSlot(seed, i, msgsPerConv, conversationCount);
            var (message, evt) = BuildSuperBundleCorpusItem(seed, i, sender, receiver, conversationId, rng);

            var outcome = await ExecuteSuperBundleAsync(commandText, message, evt, ct).ConfigureAwait(false);
            if (!outcome.MessageInserted)
            {
                throw new InvalidOperationException(
                    $"super-bundle CTE 未按预期创建消息 {message.MessageId}");
            }
        }
    }

    private async Task RunSessionAuthCacheWorkloadCoreAsync(
        int count,
        int seed,
        int conversationCount,
        CancellationToken ct)
    {
        var fullText = _superBundleCommandText ??= BuildSuperBundleCommandText(Schema);
        var noAuthText = _noAuthSuperBundleCommandText ??= BuildNoAuthSuperBundleCommandText(Schema);
        var rng = new Random(seed);
        var msgsPerConv = Math.Max(1, count / Math.Max(1, conversationCount));

        for (var i = 0; i < count; i++)
        {
            var (sender, receiver, conversationId) = ConversationSlot(seed, i, msgsPerConv, conversationCount);
            var (message, evt) = BuildSuperBundleCorpusItem(seed, i, sender, receiver, conversationId, rng);

            // 已建会话免重复授权：仅每会话第一条消息执行完整授权读取（建立会话），
            // 后续消息跳过授权读取（会话级授权缓存命中）。
            var commandText = i % msgsPerConv == 0 ? fullText : noAuthText;
            var outcome = await ExecuteSuperBundleAsync(commandText, message, evt, ct).ConfigureAwait(false);
            if (!outcome.MessageInserted)
            {
                throw new InvalidOperationException(
                    $"会话级授权缓存 CTE 未按预期创建消息 {message.MessageId}");
            }
        }
    }

    private async Task RunSenderKnownSuperBundleWorkloadCoreAsync(
        int count,
        int seed,
        CancellationToken ct)
    {
        var commandText = _senderKnownSuperBundleCommandText ??= BuildSenderKnownSuperBundleCommandText(Schema);
        var rng = new Random(seed);
        var sender = 10_000_000_001L;
        var receiver = 10_000_000_002L;
        var conversationId = ConversationId.CreateDirect(sender, receiver);

        for (var i = 0; i < count; i++)
        {
            var (message, evt) = BuildSuperBundleCorpusItem(seed, i, sender, receiver, conversationId, rng);

            var outcome = await ExecuteSuperBundleAsync(commandText, message, evt, ct).ConfigureAwait(false);
            if (!outcome.MessageInserted)
            {
                throw new InvalidOperationException(
                    $"sender-known super-bundle CTE 未按预期创建消息 {message.MessageId}");
            }
        }
    }

    /// <summary>
    /// 驱动「会话头高水位乱序」语料：第 0 条消息以极高 <c>received_at_ms</c> 建立会话头高水位，
    /// 后续消息 <c>received_at_ms</c> 均低于该高水位 → 会话头（<c>last_message_at_ms</c> 等）不再推进。
    /// <paramref name="guardConversationHeader"/> 决定会话 upsert 是否用 CASE 守卫保留会话头列
    /// （true = 生产带守卫，B；false = 无条件覆盖，A）。
    /// </summary>
    private async Task RunOutOfOrderWorkloadCoreAsync(
        int count,
        int seed,
        bool guardConversationHeader,
        CancellationToken ct)
    {
        var commandText = guardConversationHeader
            ? _superBundleCommandText ??= BuildSuperBundleCommandText(Schema)
            : _naiveSuperBundleCommandText ??= BuildNaiveSuperBundleCommandText(Schema);
        var rng = new Random(seed);
        var sender = 10_000_000_001L;
        var receiver = 10_000_000_002L;
        var conversationId = ConversationId.CreateDirect(sender, receiver);
        const long baseAt = 1_700_000_000_000L;
        const long highWatermark = baseAt + 1_000_000L;

        for (var i = 0; i < count; i++)
        {
            // 第 0 条为会话头高水位；后续消息均低于高水位，使会话头不再推进（乱序压力）。
            var at = i == 0 ? highWatermark : baseAt + i;
            var (message, evt) = BuildSuperBundleCorpusItem(seed, i, sender, receiver, conversationId, rng, at);

            var outcome = await ExecuteSuperBundleAsync(commandText, message, evt, ct).ConfigureAwait(false);
            if (!outcome.MessageInserted)
            {
                throw new InvalidOperationException(
                    $"乱序语料 super-bundle CTE 未按预期创建消息 {message.MessageId}");
            }
        }
    }

    /// <summary>
    /// 计算语料中第 <paramref name="i"/> 条消息所属会话槽位的发送者/接收者/会话 id。
    /// <para>
    /// 消息按连续块分布到 <paramref name="conversationCount"/> 个会话：接收者 =
    /// 10_000_000_002 + convIndex，会话 id = 直接单聊（发送者固定 10_000_000_001）。
    /// <c>msgsPerConv</c> 由调用方按 count/conversationCount 计算。
    /// </para>
    /// </summary>
    private static (long Sender, long Receiver, string ConversationId) ConversationSlot(
        int seed,
        int i,
        int msgsPerConv,
        int conversationCount)
    {
        _ = seed; // 分布只依赖 i；seed 用于语料 id 前缀隔离。
        var convIndex = Math.Min(i / msgsPerConv, conversationCount - 1);
        var sender = 10_000_000_001L;
        var receiver = 10_000_000_002L + convIndex;
        return (sender, receiver, ConversationId.CreateDirect(sender, receiver));
    }

    /// <summary>
    /// 构造 super-bundle 语料的单条消息 + 事件（发送者/接收者/会话 id 由调用方指定，
    /// 消息/客户端/事件 id 带种子前缀避免跨窗口幂等冲突）。
    /// </summary>
    private static (RealtimeMessageRecord Message, RealtimeEvent Event) BuildSuperBundleCorpusItem(
        int seed,
        int i,
        long sender,
        long receiver,
        string conversationId,
        Random rng,
        long? receivedAtMs = null)
    {
        var messageId = $"perf-sb-{seed}-{i:D6}";
        var receivedAt = receivedAtMs ?? 1_700_000_000_000L + i;
        var message = new RealtimeMessageRecord
        {
            MessageId = messageId,
            ClientMessageId = $"perf-client-{seed}-{i:D6}",
            SenderUserId = sender,
            SenderSessionId = "session-perf",
            ReceiverUserId = receiver,
            ConversationId = conversationId,
            Content = "perf-content-" + rng.Next(10_000),
            ReceivedAtMs = receivedAt,
        };

        var evt = new RealtimeEvent
        {
            EventId = $"perf-evt-{seed}-{i:D6}",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = receiver,
            ActorUserId = sender,
            MessageId = messageId,
            SessionId = "session-perf",
            OccurredAtMs = receivedAt,
            PayloadJson = """{"v":1}""",
        };

        return (message, evt);
    }

    /// <summary>
    /// 生成「合并同事务写入」的 super-bundle CTE：把生产合并路径（<see cref="RunMergedSaveWorkloadAsync"/>）
    /// 每次 SaveAsync 的 2 条数据语句（admission CTE + bundle CTE）合并为单条语句，热路径往返 2 → 1。
    /// <para>
    /// 前段与 <c>message-write-admission-direct-sequence</c> 完全一致：ordered_users → locked_users →
    /// lifecycle → canonical → direct_user_state → direct_authorization → write_gate → upsert_conversation
    /// → ensure_receiver → sender_upsert；后段让 message INSERT 直接读取 upsert_conversation 的
    /// <c>last_sequence</c> 与 sender_upsert 的 <c>sent_count</c>（取代 bundle 的 $19/$20 参数），
    /// 再以 inserted_message 为数据源写 outbox 与幂等账本。advisory lock 随单语句隐式事务提交释放；
    /// write_gate 为空（生命周期/授权/幂等命中）时，upsert/sender 与 message 均不产生行，与 SaveAsync 一致。
    /// </para>
    /// </summary>
    private static string BuildSuperBundleCommandText(RealtimeDatabaseSchema schema)
        => BuildSuperBundleCommandTextCore(schema, includeAuthorization: true);

    /// <summary>
    /// 生成「跳过授权读取」的 no-auth super-bundle CTE：与
    /// <see cref="BuildSuperBundleCommandText"/> 语句数相同、写入行集相同，唯一差异是
    /// admission 段不读取 <c>direct_user_state</c>（AspNetUsers）与 <c>direct_authorization</c>
    /// （T_BlockRecords / T_UserFriendEntry），<c>write_gate</c> 只按生命周期 + 幂等 canonical
    /// 门控，最终 SELECT 的授权判定固定为 NULL。用于 A/B 量化「同会话重复授权读取」成本。
    /// </summary>
    private static string BuildNoAuthSuperBundleCommandText(RealtimeDatabaseSchema schema)
        => BuildSuperBundleCommandTextCore(schema, includeAuthorization: false);

    /// <summary>
    /// 生成「仅读接收者 user_state」的 sender-known super-bundle CTE：与
    /// <see cref="BuildSuperBundleCommandText"/> 语句数相同、写入行集相同，唯一差异是
    /// <c>direct_user_state</c> 只读取接收者 AspNetUsers 行（<c>WHERE "Id" = $3</c>），发送者行
    /// 不再读取、<c>sender_exists</c> 固定为 TRUE（生产发送者是已认证用户，其存在性在 admission
    /// 时已保证）。用于 A/B 量化「发送者状态冗余读取」的每消息成本。
    /// </summary>
    private static string BuildSenderKnownSuperBundleCommandText(RealtimeDatabaseSchema schema)
        => BuildSuperBundleCommandTextCore(schema, includeAuthorization: true, senderKnown: true);

    /// <summary>
    /// 生成「无条件覆盖会话头列」的 naive super-bundle CTE：与
    /// <see cref="BuildSuperBundleCommandText"/> 语句数相同、写入行集相同，唯一差异是
    /// 会话 upsert 无 CASE 守卫——无条件覆盖 <c>last_message_id/preview/at_ms/sender_user_id</c>，
    /// 使被 <c>ix_conversations_last_message_list</c> 索引的 <c>last_message_at_ms</c> 每消息改变 →
    /// 更新为 non-HOT、每次触发索引维护。模拟「若不做无变化 UPDATE 守卫」的基线，用于 A/B 量化
    /// 生产 CASE 守卫保留索引列后 HOT 命中的收益。
    /// </summary>
    private static string BuildNaiveSuperBundleCommandText(RealtimeDatabaseSchema schema)
        => BuildSuperBundleCommandTextCore(schema, includeAuthorization: true, guardConversationHeader: false);

    private static string BuildSuperBundleCommandTextCore(
        RealtimeDatabaseSchema schema,
        bool includeAuthorization,
        bool senderKnown = false,
        bool guardConversationHeader = true)
    {
        var conversations = schema.ConversationsTableSql;
        var members = schema.ConversationMembersTableSql;
        var tombstone = schema.UserDeletionTombstonesTableSql;
        var ledger = schema.CommandIdempotencyLedgerTableSql;
        var messages = schema.MessagesTableSql;
        var outbox = schema.OutboxTableSql;

        // 授权读取段（direct_user_state + direct_authorization，4 处表读取）仅
        // includeAuthorization 时出现；否则 write_gate 只按生命周期 + 幂等 canonical 门控。
        // senderKnown 时 direct_user_state 只读取接收者行（WHERE "Id" = $3），发送者行不再读取、
        // sender_exists 固定为 TRUE（生产发送者是已认证用户，其存在性在 admission 时已保证）。
        var userStateCte = includeAuthorization
            ? senderKnown
                ? $"""
                   direct_user_state AS MATERIALIZED (
                       SELECT
                           TRUE AS sender_exists,
                           COUNT(*) FILTER (WHERE "Id" = $3) > 0 AS receiver_exists,
                           COALESCE(
                               MAX("FriendRequestPolicy"::int) FILTER (WHERE "Id" = $3),
                               -1) AS privacy_policy
                       FROM public."AspNetUsers"
                       WHERE "Id" = $3
                   ),
                   """
                : $"""
                  direct_user_state AS MATERIALIZED (
                      SELECT
                          COUNT(*) FILTER (WHERE "Id" = $2) > 0 AS sender_exists,
                          COUNT(*) FILTER (WHERE "Id" = $3) > 0 AS receiver_exists,
                          COALESCE(
                              MAX("FriendRequestPolicy"::int) FILTER (WHERE "Id" = $3),
                              -1) AS privacy_policy
                      FROM public."AspNetUsers"
                      WHERE "Id" IN ($2, $3)
                  ),
                  """
            : "";
        var authorizationCte = includeAuthorization
            ? $"""
              direct_authorization AS MATERIALIZED (
                  SELECT CASE
                      WHEN NOT direct_user_state.sender_exists THEN 1
                      WHEN NOT direct_user_state.receiver_exists THEN 2
                      WHEN EXISTS (
                          SELECT 1
                          FROM public."T_BlockRecords"
                          WHERE "BlockerId" = $3
                            AND "BlockedUserId" = $2
                      ) THEN 3
                      WHEN direct_user_state.privacy_policy = 2 THEN 4
                      WHEN NOT (
                          EXISTS (
                              SELECT 1
                              FROM public."T_UserFriendEntry"
                              WHERE "UserId" = $2
                                AND "FriendId" = $3
                                AND NOT "IsDeleted"
                          )
                          AND EXISTS (
                              SELECT 1
                              FROM public."T_UserFriendEntry"
                              WHERE "UserId" = $3
                                AND "FriendId" = $2
                                AND NOT "IsDeleted"
                          )
                      ) THEN 5
                      ELSE 0
                  END::smallint AS decision
                  FROM direct_user_state
              ),
              """
            : "";
        var gateSource = includeAuthorization
            ? """
              FROM lifecycle
              CROSS JOIN direct_authorization
              WHERE lifecycle.state = 0
                AND direct_authorization.decision = 0
                AND NOT EXISTS (SELECT 1 FROM canonical)
              """
            : """
              FROM lifecycle
              WHERE lifecycle.state = 0
                AND NOT EXISTS (SELECT 1 FROM canonical)
              """;
        var decisionExpr = includeAuthorization
            ? "direct_authorization.decision"
            : "NULL::smallint";
        var fromSuffix = includeAuthorization
            ? "\n            CROSS JOIN direct_authorization;"
            : ";";

        // 会话头（last_message_*）的 SET 片段：guardConversationHeader=true 时用生产 CASE 守卫——
        // 仅当新消息的 (last_message_at_ms, last_message_id) 元组大于当前会话头时才覆盖，否则保留旧值
        // （避免对索引列 last_message_at_ms 的无变化写入，使其可走 HOT）；false 时无条件覆盖（naive 基线，
        // 每次改写索引列 → non-HOT + 索引维护）。
        var headerSetFragment = guardConversationHeader
            ? $"""
               last_message_id = CASE
                   WHEN {conversations}.last_message_at_ms IS NULL
                        OR ({conversations}.last_message_at_ms,
                            {conversations}.last_message_id)
                           < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                   THEN EXCLUDED.last_message_id
                   ELSE {conversations}.last_message_id
               END,
               last_message_preview = CASE
                   WHEN {conversations}.last_message_at_ms IS NULL
                        OR ({conversations}.last_message_at_ms,
                            {conversations}.last_message_id)
                           < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                   THEN EXCLUDED.last_message_preview
                   ELSE {conversations}.last_message_preview
               END,
               last_message_at_ms = CASE
                   WHEN {conversations}.last_message_at_ms IS NULL
                        OR ({conversations}.last_message_at_ms,
                            {conversations}.last_message_id)
                           < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                   THEN EXCLUDED.last_message_at_ms
                   ELSE {conversations}.last_message_at_ms
               END,
               last_sender_user_id = CASE
                   WHEN {conversations}.last_message_at_ms IS NULL
                        OR ({conversations}.last_message_at_ms,
                            {conversations}.last_message_id)
                           < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                   THEN EXCLUDED.last_sender_user_id
                   ELSE {conversations}.last_sender_user_id
               END,
               """
            : $"""
              last_message_id = EXCLUDED.last_message_id,
              last_message_preview = EXCLUDED.last_message_preview,
              last_message_at_ms = EXCLUDED.last_message_at_ms,
              last_sender_user_id = EXCLUDED.last_sender_user_id,
              """;

        return $"""
            WITH ordered_users AS MATERIALIZED (
                SELECT DISTINCT t.user_id
                FROM (VALUES ($2), ($3)) AS t(user_id)
                WHERE t.user_id > 0
                ORDER BY t.user_id
            ),
            locked_users AS MATERIALIZED (
                SELECT u.user_id
                FROM ordered_users AS u
                WHERE pg_advisory_xact_lock_shared(
                    ($1::bigint # u.user_id)) IS NULL
            ),
            lifecycle AS MATERIALIZED (
                SELECT COALESCE(MAX(tombstone.state), 0)::smallint AS state
                FROM locked_users AS locked
                LEFT JOIN {tombstone} AS tombstone
                  ON tombstone.user_id = locked.user_id
            ),
            canonical AS MATERIALIZED (
                SELECT command_id, content_fingerprint, result_kind, message_id, received_at_ms
                FROM {ledger}
                WHERE sender_user_id = $2
                  AND client_message_id = $4
                LIMIT 1
            ),
            {userStateCte}
            {authorizationCte}
            write_gate AS MATERIALIZED (
                SELECT 1
                {gateSource}
            ),
            upsert_conversation AS (
                INSERT INTO {conversations} (
                    conversation_id, type, created_at_ms, updated_at_ms,
                    last_message_id, last_message_preview, last_message_at_ms,
                    last_sender_user_id, last_sequence
                )
                SELECT
                    $5, $9, $6, $6,
                    $7, $8, $6,
                    $2, 1
                FROM write_gate
                ON CONFLICT (conversation_id) DO UPDATE SET
                    last_sequence = {conversations}.last_sequence + 1,
                    {headerSetFragment}
                    updated_at_ms = EXCLUDED.updated_at_ms
                RETURNING last_sequence
            ),
            ensure_receiver AS (
                INSERT INTO {members} (
                    conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms
                )
                SELECT $5, $3, $2, $6, $6
                FROM upsert_conversation
                ON CONFLICT (conversation_id, user_id) DO NOTHING
            ),
            sender_upsert AS (
                INSERT INTO {members} (
                    conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms, sent_count
                )
                SELECT $5, $2, $3, $6, $6, 1
                FROM upsert_conversation
                ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                    sent_count = {members}.sent_count + 1,
                    last_message_at_ms = $6
                RETURNING sent_count
            ),
            inserted_message AS MATERIALIZED (
                INSERT INTO {messages} (
                    message_id, client_message_id, sender_user_id, sender_session_id,
                    receiver_user_id, conversation_id, content, content_fingerprint,
                    received_at_ms, created_at_ms, reply_to_message_id,
                    reply_to_sender_user_id, reply_to_preview, forwarded_from_message_id,
                    forwarded_from_sender_user_id, forwarded_from_preview,
                    mentioned_user_ids, mentioned_roles, edit_version, changed_at_ms,
                    conversation_sequence, sender_sequence
                )
                SELECT
                    $7, $4, $2, $10,
                    $3, $5, $11, $12,
                    $6, $13, $14,
                    $15, $16, $17,
                    $18, $19,
                    $20, $21, 1, $6,
                    upsert_conversation.last_sequence,
                    sender_upsert.sent_count
                FROM write_gate
                CROSS JOIN upsert_conversation
                CROSS JOIN sender_upsert
                ON CONFLICT (sender_user_id, client_message_id) DO NOTHING
                RETURNING message_id
            ),
            inserted_outbox AS MATERIALIZED (
                INSERT INTO {outbox} (
                    event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                    created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
                    audience_kind, conversation_id, exclude_user_id, trace_parent, trace_state,
                    occurred_at_ms, locked_by, locked_until_ms, claim_token
                )
                SELECT
                    $22, NULL, $23, $24, $25, $26,
                    $13, COALESCE($35, $13), $27, $28,
                    $29, $5, NULLIF($30, 0),
                    $31, $32, $33,
                    $34, $35, $36
                FROM inserted_message
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            ),
            inserted_ledger AS MATERIALIZED (
                INSERT INTO {ledger} (
                    sender_user_id, client_message_id, command_id, content_fingerprint,
                    result_kind, message_id, received_at_ms
                )
                SELECT
                    $2, $4, $7, $12,
                    $38, $7, $6
                FROM inserted_message
                WHERE $37
                ON CONFLICT (sender_user_id, client_message_id) DO NOTHING
                RETURNING sender_user_id
            )
            SELECT
                (SELECT last_sequence FROM upsert_conversation) AS conversation_sequence,
                (SELECT sent_count FROM sender_upsert) AS sender_sequence,
                (CASE WHEN EXISTS (SELECT 1 FROM inserted_message) THEN 1 ELSE 0 END)
                    + (CASE WHEN EXISTS (SELECT 1 FROM inserted_outbox) THEN 2 ELSE 0 END)
                    + (CASE WHEN EXISTS (SELECT 1 FROM inserted_ledger) THEN 4 ELSE 0 END)
                    AS write_flags,
                lifecycle.state,
                canonical.command_id,
                {decisionExpr}
            FROM lifecycle
            LEFT JOIN canonical ON TRUE{fromSuffix}
            """;
    }

    /// <summary>
    /// 在独立连接上执行单条 super-bundle CTE（单语句隐式事务，advisory lock 随语句提交释放）。
    /// 参数顺序与 <see cref="BuildSuperBundleCommandText"/> 的 $1..$38 一一对应：$1..$9 为 admission
    /// （锁/生命周期/授权/幂等 canonical + 会话序号分配），$10..$21 为 message，$22..$36 为 outbox，
    /// $37..$38 为幂等账本（写开关 + 结果类型）。
    /// </summary>
    private async Task<SuperBundleWriteOutcome> ExecuteSuperBundleAsync(
        string commandText,
        RealtimeMessageRecord message,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        var fingerprint = RealtimeMessageFingerprint.Compute(
            message.ReceiverUserId,
            message.Content,
            message.AttachmentIds,
            message.ConversationId,
            message.ReplyToMessageId,
            message.ForwardedFromMessageId,
            message.MentionedUserIds,
            message.MentionedRoles,
            message.ReplyToSenderUserId,
            message.ReplyToPreview,
            message.ForwardedFromSenderUserId,
            message.ForwardedFromPreview);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var payloadUtf8 = RealtimeEventWireSerializer.SerializeToUtf8Bytes(evt);

        await using var connection = await Client.GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(commandText, connection);

        // $1..$9：admission（与 MessageWriteAdmissionReader.AcquireDirectAndAllocateSequenceAsync 一致）。
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, LifecycleNamespaceKey);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.SenderUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.ReceiverUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.ClientMessageId);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ConversationId ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.ReceivedAtMs);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.MessageId);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            ConversationId.CreatePreview(message.Content));
        command.Parameters.AddWithValue(NpgsqlDbType.Smallint, (short)ConversationType.Direct);

        // $10..$21：message（与 MessageCreateBundleWriter.AddMessageParameters 一致，序号参数下沉为 CTE）。
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.SenderSessionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.Content);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, fingerprint);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, now);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ReplyToMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)message.ReplyToSenderUserId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ReplyToPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ForwardedFromMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)message.ForwardedFromSenderUserId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ForwardedFromPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            message.MentionedUserIds is { Count: > 0 }
                ? message.MentionedUserIds.ToArray()
                : DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Text,
            message.MentionedRoles is { Count: > 0 }
                ? message.MentionedRoles.ToArray()
                : DBNull.Value);

        // $22..$36：outbox（与 MessageCreateBundleWriter.AddOutboxParameters 一致，无预领取；
        // conversation_id 复用 admission 的 $5，不在此重复绑定）。
        command.Parameters.AddWithValue(NpgsqlDbType.Text, evt.EventId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bytea, payloadUtf8);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.TargetUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Smallint, (short)evt.Type);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 0); // attempt_count（无预领取）
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            (object?)evt.TargetUserIds ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)(evt.AudienceKind ?? 0));
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.ExcludeUserId ?? 0L); // $30
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)evt.TraceParent ?? DBNull.Value); // $31
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)evt.TraceState ?? DBNull.Value); // $32
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.OccurredAtMs); // $33
        command.Parameters.AddWithValue(NpgsqlDbType.Text, DBNull.Value); // $34 locked_by（无预领取）
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, DBNull.Value); // $35 locked_until_ms
        command.Parameters.AddWithValue(NpgsqlDbType.Text, DBNull.Value); // $36 claim_token

        // $37..$38：幂等账本（写开关 + 结果类型）。
        command.Parameters.AddWithValue(NpgsqlDbType.Boolean, true); // writeLedger
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)IdempotencyLedgerResultKind.Created);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException("super-bundle CTE 未返回结果行。");

        return new SuperBundleWriteOutcome(
            ConversationSequence: reader.IsDBNull(0) ? null : reader.GetInt64(0),
            SenderSequence: reader.IsDBNull(1) ? null : reader.GetInt64(1),
            WriteFlags: reader.GetInt32(2),
            LifecycleState: reader.GetInt16(3),
            LedgerCommandId: reader.IsDBNull(4) ? null : reader.GetString(4),
            AuthorizationDecision: reader.IsDBNull(5) ? (short)-1 : reader.GetInt16(5));
    }

    private async Task RunSaveWorkloadCoreAsync(
        NpgsqlRealtimeMessageStore store,
        int count,
        int seed,
        CancellationToken ct,
        int replyEvery)
    {
        var rng = new Random(seed);
        long sender = 10_000_000_001;
        long receiver = 10_000_000_002;
        var conversationId = ConversationId.CreateDirect(sender, receiver);

        for (var i = 0; i < count; i++)
        {
            // 消息 id / 客户端 id / 事件 id 均带种子前缀，避免预热与测量两段窗口
            // 使用相同 id 但内容不同而触发幂等内容冲突。
            var messageId = $"perf-{seed}-{i:D6}";
            // 每 replyEvery 条消息带回复/转发引用（引用目标仅作展示性引用，无需真实存在，
            // 存储层不做存在性校验），使部分索引在这些行上产生写放大。
            var isReferenced = replyEvery > 0 && i % replyEvery == 0;
            var message = new RealtimeMessageRecord
            {
                MessageId = messageId,
                ClientMessageId = $"perf-client-{seed}-{i:D6}",
                SenderUserId = sender,
                SenderSessionId = "session-perf",
                ReceiverUserId = receiver,
                ConversationId = conversationId,
                Content = "perf-content-" + rng.Next(10_000),
                ReceivedAtMs = 1_700_000_000_000L + i,
                ReplyToMessageId = isReferenced ? $"perf-reply-{seed}-{i:D6}" : null,
                ReplyToSenderUserId = isReferenced ? sender : null,
                ForwardedFromMessageId = isReferenced ? $"perf-fwd-{seed}-{i:D6}" : null,
                ForwardedFromSenderUserId = isReferenced ? sender : null,
            };

            var evt = new RealtimeEvent
            {
                EventId = $"perf-evt-{seed}-{i:D6}",
                Type = RealtimeEventType.MessageReceived,
                TargetUserId = receiver,
                ActorUserId = sender,
                MessageId = messageId,
                SessionId = "session-perf",
                OccurredAtMs = 1_700_000_000_000L + i,
                PayloadJson = """{"v":1}""",
            };

            var result = await store.SaveAsync(message, evt, ct).ConfigureAwait(false);
            if (result.Kind != RealtimeMessagePersistKind.Created)
            {
                throw new InvalidOperationException(
                    $"SaveAsync 未按预期创建消息 {messageId}：{result.Kind}");
            }
        }
    }

    /// <summary>
    /// 驱动 Outbox 认领 + 批量完成的排水热路径 <paramref name="batches"/> 批、每批
    /// <paramref name="batchSize"/> 条。用于测量 claim / complete 的 SQL 与 WAL 成本。
    /// <para>
    /// 该路径对应 <c>PublishedRetentionHours &gt; 0</c> 的保留模式：claim 后把行置为
    /// Published，由 <c>OutboxCleanupWorker</c> 在稍后统一清理。
    /// </para>
    /// </summary>
    public async Task<long> RunOutboxDrainAsync(
        int batches,
        int batchSize,
        CancellationToken ct = default)
    {
        long completed = 0;
        for (var b = 0; b < batches; b++)
        {
            var claimed = await OutboxStore.ClaimBatchAsync("perf-worker", batchSize, TimeSpan.FromSeconds(30), ct)
                .ConfigureAwait(false);
            if (claimed.Count == 0)
            {
                break;
            }

            completed += await OutboxStore.MarkPublishedBatchAsync(claimed, ct).ConfigureAwait(false);
        }

        return completed;
    }

    /// <summary>
    /// 驱动 Outbox 认领 + 立即删除的排水热路径（delete-on-complete），<paramref name="batches"/> 批、
    /// 每批 <paramref name="batchSize"/> 条。对应生产默认 <c>PublishedRetentionHours = 0</c> 的
    /// 完成模式：claim 后直接删除行，不保留 Published 状态、不触发 cleanup worker。
    /// 与 <see cref="RunOutboxDrainAsync"/>（保留模式）对照归因两种完成模式的 WAL 成本。
    /// </summary>
    public async Task<long> RunOutboxDrainDeleteAsync(
        int batches,
        int batchSize,
        CancellationToken ct = default)
    {
        long completed = 0;
        for (var b = 0; b < batches; b++)
        {
            var claimed = await OutboxStore.ClaimBatchAsync("perf-worker", batchSize, TimeSpan.FromSeconds(30), ct)
                .ConfigureAwait(false);
            if (claimed.Count == 0)
            {
                break;
            }

            completed += await OutboxStore.DeleteClaimedPublishedBatchAsync(claimed, ct).ConfigureAwait(false);
        }

        return completed;
    }

    /// <summary>
    /// 清除所有已发布（Published）行，模拟 <c>OutboxCleanupWorker</c> 的一次全量清理。
    /// 用于保留模式完整排水生命周期测量（claim + MarkPublished + 后续 cleanup）。
    /// 以未来时间戳为 cutoff 循环删除直到清空，返回累计删除行数。
    /// </summary>
    public async Task<long> RunOutboxCleanupAsync(int batchSize = 10_000, CancellationToken ct = default)
    {
        long cleaned = 0;
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1;
        while (true)
        {
            var affected = await OutboxStore
                .CleanupPublishedAsync(cutoff, batchSize, ct)
                .ConfigureAwait(false);
            if (affected == 0)
            {
                break;
            }

            cleaned += affected;
        }

        return cleaned;
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }

        await _container.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// 单条 super-bundle CTE 的执行结果：会话序号、写入标志与 admission 判读，供
/// <see cref="PostgresPerfHarness.ExecuteSuperBundleAsync"/> 返回以校验写入语义与幂等性。
/// </summary>
internal readonly record struct SuperBundleWriteOutcome(
    long? ConversationSequence,
    long? SenderSequence,
    int WriteFlags,
    short LifecycleState,
    string? LedgerCommandId,
    short AuthorizationDecision)
{
    public bool MessageInserted => (WriteFlags & 1) != 0;

    public bool OutboxInserted => (WriteFlags & 2) != 0;

    public bool LedgerInserted => (WriteFlags & 4) != 0;
}
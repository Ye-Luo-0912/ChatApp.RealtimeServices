using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
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

    private RealtimeDatabaseClient? _client;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlRealtimeMessageStore? _messageStore;
    private NpgsqlRealtimeOutboxStore? _outboxStore;

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

    public RealtimeDatabaseClient Client =>
        _client ?? throw new InvalidOperationException("Harness 未初始化。");

    public RealtimeDatabaseSchema Schema =>
        _schema ?? throw new InvalidOperationException("Harness 未初始化。");

    public NpgsqlRealtimeMessageStore MessageStore =>
        _messageStore ?? throw new InvalidOperationException("Harness 未初始化。");

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
    /// 用固定语料 + 固定随机种子驱动真实 <see cref="NpgsqlRealtimeMessageStore.SaveAsync"/>
    /// 热路径 <paramref name="count"/> 条消息。每条消息使用独立会话/客户端编号，避免幂等命中。
    /// </summary>
    public async Task RunSaveWorkloadAsync(
        int count,
        int seed,
        CancellationToken ct = default)
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

            var result = await MessageStore.SaveAsync(message, evt, ct).ConfigureAwait(false);
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

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }

        await _container.DisposeAsync().ConfigureAwait(false);
    }
}
using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// Outbox 关键路径基准：UNNEST 批量 INSERT 与 Claim（FOR UPDATE SKIP LOCKED）。
/// 使用 Testcontainers 启动 PostgreSQL 16，迁移完整 schema 后测量。
/// 六-1：方法返回值为单次操作的 SQL 命令数（通过 NpgsqlSqlCommandCounter 计数），
/// BenchmarkDotNet 会自动在 "Return" 列展示，用于性能门禁的 SQL 往返次数基线。
/// </summary>
[MemoryDiagnoser]
[GcServer]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class OutboxInsertBenchmarks
{
    private PostgreSqlContainer _container = null!;
    private RealtimeDatabaseClient _client = null!;
    private RealtimeDatabaseSchema _schema = null!;
    private NpgsqlRealtimeOutboxStore _store = null!;
    private NpgsqlConnection _insertConnection = null!;

    /// <summary>单次 INSERT 的事件数量。</summary>
    [Params(1, 10, 50)]
    public int EventCount { get; set; }

    [GlobalSetup]
    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _schema = new RealtimeDatabaseSchema("realtime_bench_outbox");
        _client = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);
        _store = new NpgsqlRealtimeOutboxStore(_client, _schema);

        await using var migrateConnection = await _client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger.Instance)
            .MigrateAsync(migrateConnection);

        _insertConnection = new NpgsqlConnection(_container.GetConnectionString());
        await _insertConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task DisposeAsync()
    {
        await _insertConnection.DisposeAsync();
        await _client.DisposeAsync();
        await _container.DisposeAsync();
    }

    /// <summary>每次迭代前清空 outbox 并预置一批 Pending 行供 Claim 基准使用。</summary>
    [IterationSetup]
    public void SetupIteration()
    {
        SetupIterationCore().GetAwaiter().GetResult();
    }

    private async Task SetupIterationCore()
    {
        await using var truncate = new NpgsqlCommand(
            $"TRUNCATE {_schema.OutboxTableSql};", _insertConnection);
        await truncate.ExecuteNonQueryAsync();

        // 预置一批 Pending 行，供 ClaimBatch 基准认领。
        // generate_series 单语句批量插入，避免逐行往返。
        // 六-2：种子写 payload_utf8（bytea）而非 payload_json，使 Claim 走零解析 UTF8 路径。
        await using var seed = new NpgsqlCommand(
            $"""
             INSERT INTO {_schema.OutboxTableSql} (
                 event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             )
             SELECT 'pool-' || g, NULL, @payload_utf8, 12, 5, 0, 0, 0, 0
             FROM generate_series(1, 5000) AS g;
             """,
            _insertConnection);
        seed.Parameters.Add("payload_utf8", NpgsqlDbType.Bytea).Value = System.Text.Encoding.UTF8.GetBytes("{}");
        await seed.ExecuteNonQueryAsync();
    }

    /// <summary>UNNEST 批量 INSERT（payload_utf8 预序列化，payload_json 写 NULL）。返回 SQL 命令数。</summary>
    [Benchmark(Description = "Outbox INSERT (UNNEST, payload_utf8 only)")]
    public async Task<int> InsertOutboxMany()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        var events = Enumerable.Range(0, EventCount)
            .Select(_ => new RealtimeEvent
            {
                EventId = Guid.NewGuid().ToString("N"),
                Type = RealtimeEventType.MessageReceived,
                TargetUserId = 42,
                OccurredAtMs = 1_700_000_000_000L,
                PayloadJson = """{"v":1}""",
            })
            .ToArray();

        await using var tx = await _insertConnection.BeginTransactionAsync();
        await OutboxInsertHelper.InsertManyAsync(
            _insertConnection, tx, _schema, events, CancellationToken.None);
        await tx.CommitAsync();
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    /// <summary>Claim 一批 10 条 Pending 行（FOR UPDATE SKIP LOCKED，0 JSON 反序列化）。返回 SQL 命令数。</summary>
    [Benchmark(Description = "Outbox Claim (0 JSON parse)")]
    public async Task<int> ClaimBatch()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store.ClaimBatchAsync("bench", 10, TimeSpan.FromSeconds(30));
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }
}
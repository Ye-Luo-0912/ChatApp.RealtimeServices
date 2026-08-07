using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 门禁4：SyncBootstrap 禁止 N+1 断言。
/// <para>
/// 多会话 catch-up 历史批量查询必须走 <c>QueryCatchUpsAsync</c> 的单条 UNNEST 批量 SQL，
/// 而不是对每个会话逐次查询。N 个会话的全量 catch-up 只允许 1 次 SQL。
/// 若 SQL 次数随会话数增长，说明 SyncBootstrap 退化为 N+1。
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class SyncBootstrapBenchmarks
{
    private const int SqlBatchCatchUps = 1;

    private const int ConversationCount = 3;
    private const long UserId = 9001;
    private const long PeerUserId = 9002;

    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlConnection? _connection;
    private NpgsqlRealtimeMessageHistoryStore? _historyStore;

    [GlobalSetup]
    public void Initialize()
    {
        InitializeAsync().GetAwaiter().GetResult();
    }

    private async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);
        _schema = new RealtimeDatabaseSchema("realtime_bench_sync");

        await using var migrateConnection = await _dbClient.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        _historyStore = new NpgsqlRealtimeMessageHistoryStore(_dbClient, _schema);

        await SeedDataAsync();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        CleanupAsync().GetAwaiter().GetResult();
    }

    private async Task CleanupAsync()
    {
        if (_connection is not null)
            await _connection.DisposeAsync();
        if (_dbClient is not null)
            await _dbClient.DisposeAsync();
        if (_container is not null)
            await _container.DisposeAsync();
    }

    private async Task SeedDataAsync()
    {
        var schema = _schema!;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (var c = 1; c <= ConversationCount; c++)
        {
            var convId = $"bench-sync-conv-{c}";
            await using (var conv = new NpgsqlCommand(
                             $"""INSERT INTO {schema.ConversationsTableSql} (conversation_id, type, created_at_ms, updated_at_ms) VALUES (@conv, 1, @now, @now) ON CONFLICT DO NOTHING;""",
                             _connection))
            {
                conv.Parameters.AddWithValue("conv", convId);
                conv.Parameters.AddWithValue("now", nowMs);
                await conv.ExecuteNonQueryAsync();
            }

            await using (var member = new NpgsqlCommand(
                             $"""INSERT INTO {schema.ConversationMembersTableSql} (conversation_id, user_id, last_read_sequence, last_read_at_ms, left_at_ms, joined_at_ms) VALUES (@conv, @user, 0, NULL, NULL, @join), (@conv, @peer, 0, NULL, NULL, @join) ON CONFLICT DO NOTHING;""",
                             _connection))
            {
                member.Parameters.AddWithValue("conv", convId);
                member.Parameters.AddWithValue("user", UserId);
                member.Parameters.AddWithValue("peer", PeerUserId);
                member.Parameters.AddWithValue("join", nowMs - 120000);
                await member.ExecuteNonQueryAsync();
            }

            // 每个会话 5 条消息，供全量 catch-up 拉取。
            await using (var msg = new NpgsqlCommand(
                             $"""
                              INSERT INTO {schema.MessagesTableSql}
                                  (message_id, client_message_id, sender_user_id, sender_session_id,
                                   receiver_user_id, content, received_at_ms, created_at_ms,
                                   conversation_id, conversation_sequence, changed_at_ms)
                              SELECT
                                  'bench-sync-msg-' || {c} || '-' || g,
                                  'bench-sync-cmsg-' || {c} || '-' || g,
                                  @peer, 'sess',
                                  @user, 'payload-' || g,
                                  @now - 1000 + g, @now - 1000 + g, @conv, g, @now - 1000 + g
                              FROM generate_series(1, 5) AS g
                              ON CONFLICT DO NOTHING;
                              """,
                             _connection))
            {
                msg.Parameters.AddWithValue("peer", PeerUserId);
                msg.Parameters.AddWithValue("user", UserId);
                msg.Parameters.AddWithValue("now", nowMs);
                msg.Parameters.AddWithValue("conv", convId);
                await msg.ExecuteNonQueryAsync();
            }
        }
    }

    [Benchmark(Description = "SyncBootstrap catch-up: 3 convs full fetch (1 SQL, no N+1)")]
    public async Task<int> QueryCatchUps()
    {
        var queries = Enumerable.Range(1, ConversationCount)
            .Select(static c => new HistoryCatchUpQuery
            {
                ConversationId = $"bench-sync-conv-{c}",
                Take = 21
            })
            .ToArray();

        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _historyStore!.QueryCatchUpsAsync(UserId, queries);
        return AssertSqlCount(SqlBatchCatchUps);
    }

    private int AssertSqlCount(int upperBound)
    {
        var count = NpgsqlSqlCommandCounter.GetCommandCount();
        if (count > upperBound)
        {
            throw new InvalidOperationException(
                $"SQL 命令数 {count} 超过门禁上限 {upperBound}。SyncBootstrap catch-up 必须为单条批量查询，不得退化为 N+1。");
        }
        return count;
    }
}
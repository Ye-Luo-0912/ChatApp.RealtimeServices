using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ReadReceiptBenchmarks : IAsyncLifetime
{
    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlRealtimeReadReceiptStore? _readReceiptStore;

    private const string ConversationId = "bench-rr-conv-001";
    private const long SenderUserId = 20002;
    private const long ReaderUserId1 = 20003;
    private const long ReaderUserId2 = 20004;
    private const long LateJoinerUserId = 20005;
    private const long MessageSequence = 42;

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(_container.GetConnectionString(), NullLogger<RealtimeDatabaseClient>.Instance);
        _schema = new RealtimeDatabaseSchema("realtime_bench_rr");

        await using var migrateConnection = await _dbClient!.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection, CancellationToken.None);

        await SeedDataAsync();
        _readReceiptStore = new NpgsqlRealtimeReadReceiptStore(_dbClient, _schema);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    [Benchmark]
    public async Task<int> GetReaders_FirstPage()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _readReceiptStore!.GetReadersAsync(ConversationId, MessageSequence, SenderUserId, null, 50);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> GetReadSummary()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _readReceiptStore!.GetReadSummaryAsync(ConversationId, MessageSequence, SenderUserId);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    private async Task SeedDataAsync()
    {
        await using var conn = await _dbClient!.GetDataSource().OpenConnectionAsync();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var convCmd = new NpgsqlCommand(
            $"""INSERT INTO {_schema!.ConversationsTableSql} (conversation_id, type, created_at_ms, updated_at_ms) VALUES (@conv, 1, @now, @now) ON CONFLICT DO NOTHING;""", conn);
        convCmd.Parameters.AddWithValue("conv", ConversationId);
        convCmd.Parameters.AddWithValue("now", nowMs);
        await convCmd.ExecuteNonQueryAsync();

        await using var msgCmd = new NpgsqlCommand(
            $"""INSERT INTO {_schema.MessagesTableSql} (conversation_id, conversation_sequence, sender_user_id, content, created_at_ms) VALUES (@conv, @seq, @sender, 'bench', @created) ON CONFLICT DO NOTHING;""", conn);
        msgCmd.Parameters.AddWithValue("conv", ConversationId);
        msgCmd.Parameters.AddWithValue("seq", MessageSequence);
        msgCmd.Parameters.AddWithValue("sender", SenderUserId);
        msgCmd.Parameters.AddWithValue("created", nowMs - 60000);
        await msgCmd.ExecuteNonQueryAsync();

        await using var memberCmd = new NpgsqlCommand(
            $"""INSERT INTO {_schema.ConversationMembersTableSql} (conversation_id, user_id, last_read_sequence, last_read_at_ms, left_at_ms, joined_at_ms) VALUES (@conv, @r1, @seq, @now, NULL, @jb), (@conv, @r2, @seq, @now, NULL, @jb), (@conv, @lj, @seq, @now, NULL, @ja), (@conv, @sender, NULL, NULL, NULL, @jb) ON CONFLICT DO NOTHING;""", conn);
        memberCmd.Parameters.AddWithValue("conv", ConversationId);
        memberCmd.Parameters.AddWithValue("r1", ReaderUserId1);
        memberCmd.Parameters.AddWithValue("r2", ReaderUserId2);
        memberCmd.Parameters.AddWithValue("lj", LateJoinerUserId);
        memberCmd.Parameters.AddWithValue("sender", SenderUserId);
        memberCmd.Parameters.AddWithValue("seq", MessageSequence);
        memberCmd.Parameters.AddWithValue("now", nowMs);
        memberCmd.Parameters.AddWithValue("jb", nowMs - 120000);
        memberCmd.Parameters.AddWithValue("ja", nowMs - 30000);
        await memberCmd.ExecuteNonQueryAsync();

        await using var periodCmd = new NpgsqlCommand(
            $"""INSERT INTO {_schema.MembershipPeriodsTableSql} (conversation_id, user_id, joined_at_ms, left_at_ms) VALUES (@conv, @r1, @jb, NULL), (@conv, @r2, @jb, NULL), (@conv, @lj, @ja, NULL), (@conv, @sender, @jb, NULL) ON CONFLICT DO NOTHING;""", conn);
        periodCmd.Parameters.AddWithValue("conv", ConversationId);
        periodCmd.Parameters.AddWithValue("r1", ReaderUserId1);
        periodCmd.Parameters.AddWithValue("r2", ReaderUserId2);
        periodCmd.Parameters.AddWithValue("lj", LateJoinerUserId);
        periodCmd.Parameters.AddWithValue("sender", SenderUserId);
        periodCmd.Parameters.AddWithValue("jb", nowMs - 120000);
        periodCmd.Parameters.AddWithValue("ja", nowMs - 30000);
        await periodCmd.ExecuteNonQueryAsync();
    }
}

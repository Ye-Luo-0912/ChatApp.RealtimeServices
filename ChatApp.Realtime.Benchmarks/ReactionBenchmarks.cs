using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// Reaction 操作基准：验证单条 CTE 完成存在性检查/计数/插入/水位推进，
/// 且仅更新 messages.changed_at_ms 轻量列，不锁 messages 正文行。
/// 每次迭代用递增 user_id 触发真实 INSERT（避免 AlreadyExists 短路）。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ReactionBenchmarks
{
    private PostgreSqlContainer _container = null!;
    private RealtimeDatabaseClient _client = null!;
    private RealtimeDatabaseSchema _schema = null!;
    private NpgsqlConnection _connection = null!;
    private long _nextUserId = 6000;

    [GlobalSetup]
    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _schema = new RealtimeDatabaseSchema("realtime_bench_reaction");
        _client = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);

        await using var migrateConnection = await _client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        await SeedMessageAsync();
    }

    [GlobalCleanup]
    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
        await _client.DisposeAsync();
        await _container.DisposeAsync();
    }

    /// <summary>每次迭代前清空 reactions 表并重置 user_id 计数器。</summary>
    [IterationSetup]
    public async Task SetupIteration()
    {
        _nextUserId = 6000;
        await using var truncate = new NpgsqlCommand(
            $"TRUNCATE {_schema.MessageReactionsTableSql};", _connection);
        await truncate.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Reaction 添加 CTE：7 次往返压成 1 次，bump 仅更新 changed_at_ms 不触碰 content。
    /// </summary>
    [Benchmark(Description = "Reaction add CTE (no message body lock)")]
    public async Task AddReaction()
    {
        var userId = Interlocked.Increment(ref _nextUserId);

        await using var command = new NpgsqlCommand(
            $"""
             WITH
             existing AS (
                 SELECT 1 FROM {_schema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id = @user_id AND emoji = @emoji
                 LIMIT 1
             ),
             user_cnt AS (
                 SELECT COUNT(*)::int AS v FROM {_schema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id = @user_id
             ),
             emoji_exists_other AS (
                 SELECT 1 FROM {_schema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND emoji = @emoji AND user_id <> @user_id
                 LIMIT 1
             ),
             distinct_other AS (
                 SELECT COUNT(DISTINCT emoji)::int AS v FROM {_schema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id <> @user_id
             ),
             emoji_count_pre AS (
                 SELECT COUNT(*)::int AS v FROM {_schema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND emoji = @emoji
             ),
             decision AS (
                 SELECT CASE
                     WHEN EXISTS(SELECT 1 FROM existing) THEN 0
                     WHEN (SELECT v FROM user_cnt) >= @max_per_user THEN 1
                     WHEN NOT EXISTS(SELECT 1 FROM emoji_exists_other)
                          AND (SELECT v FROM distinct_other) >= @max_distinct THEN 1
                     ELSE 2
                 END AS status
             ),
             ins AS (
                 INSERT INTO {_schema.MessageReactionsTableSql}
                     (message_id, user_id, emoji, created_at_ms)
                 SELECT @message_id, @user_id, @emoji, @created_at_ms
                 WHERE (SELECT status FROM decision) = 2
                 ON CONFLICT (message_id, user_id, emoji) DO NOTHING
                 RETURNING 1
             ),
             bump AS (
                 UPDATE {_schema.MessagesTableSql}
                 SET changed_at_ms = GREATEST(changed_at_ms, @changed_at_ms)
                 WHERE message_id = @message_id AND EXISTS (SELECT 1 FROM ins)
             )
             SELECT
                 CASE
                     WHEN EXISTS(SELECT 1 FROM ins) THEN 2
                     ELSE (SELECT status FROM decision)
                 END;
             """,
            _connection);
        command.Parameters.AddWithValue("message_id", "bench-react-msg");
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("emoji", "+1");
        command.Parameters.AddWithValue("created_at_ms", 1_700_000_000_000L);
        command.Parameters.AddWithValue("changed_at_ms", 1_700_000_000_000L);
        command.Parameters.AddWithValue("max_per_user", 20);
        command.Parameters.AddWithValue("max_distinct", 100);

        await command.ExecuteScalarAsync();
    }

    private async Task SeedMessageAsync()
    {
        await using var tx = await _connection.BeginTransactionAsync();

        await using (var conv = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_schema.ConversationsTableSql} (
                              conversation_id, type, created_at_ms, updated_at_ms
                          ) VALUES (
                              'bench-react-conv', 1, 1, 1
                          )
                          ON CONFLICT (conversation_id) DO NOTHING;
                          """,
                         _connection, tx))
        {
            await conv.ExecuteNonQueryAsync();
        }

        await using (var msg = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_schema.MessagesTableSql} (
                              message_id, client_message_id, sender_user_id, sender_session_id,
                              receiver_user_id, conversation_id, content, received_at_ms, created_at_ms
                          ) VALUES (
                              'bench-react-msg', 'c-react', 6001, 's', 6002, 'bench-react-conv', 'react me', 1, 1
                          )
                          ON CONFLICT (message_id) DO NOTHING;
                          """,
                         _connection, tx))
        {
            await msg.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }
}

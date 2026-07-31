using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// Reaction 操作基准：纯 CTE 路径与端到端 Store 路径对比。
/// 端到端路径覆盖 advisory lock → tombstone 检查 → message_state 锁 → 权限校验 → CTE → Outbox 写入 → commit。
/// 六-1：方法返回值为单次操作的 SQL 命令数，用于性能门禁的 SQL 往返次数基线。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ReactionBenchmarks
{
    private PostgreSqlContainer _container = null!;
    private RealtimeDatabaseClient _client = null!;
    private RealtimeDatabaseSchema _schema = null!;
    private NpgsqlConnection _connection = null!;
    private NpgsqlRealtimeReactionStore _store = null!;
    private long _nextUserId = 6000;
    private long _nextEmojiSeq = 0;

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

        var policy = new PostgresConversationMessageMutationPolicy(
            NullLogger<PostgresConversationMessageMutationPolicy>.Instance);
        _store = new NpgsqlRealtimeReactionStore(_client, _schema, policy);

        await SeedMessageAsync();
    }

    [GlobalCleanup]
    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
        await _client.DisposeAsync();
        await _container.DisposeAsync();
    }

    /// <summary>每次迭代前清空 reactions + outbox 表并重置计数器。</summary>
    [IterationSetup]
    public void SetupIteration()
    {
        SetupIterationCore().GetAwaiter().GetResult();
    }

    private async Task SetupIterationCore()
    {
        _nextUserId = 6000;
        _nextEmojiSeq = 0;
        await using var truncate = new NpgsqlCommand(
            $"TRUNCATE {_schema.MessageReactionsTableSql}; TRUNCATE {_schema.OutboxTableSql};",
            _connection);
        await truncate.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// 纯 CTE 路径：7 次往返压成 1 次，bump 仅更新 changed_at_ms 不触碰 content。
    /// 返回 SQL 命令数（应为 1，若回退到多往返则说明 CTE 被拆分）。
    /// </summary>
    [Benchmark(Description = "Reaction add CTE only (1 SQL roundtrip)")]
    public async Task<int> AddReactionCte()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
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
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    /// <summary>
    /// 六-4：端到端 Store 路径。通过真实 NpgsqlRealtimeReactionStore.AddAsync 执行，
    /// 覆盖 advisory lock → tombstone 检查 → message_state 锁 → 权限校验 → CTE → Outbox 写入 → commit。
    /// 用递增 emoji 触发真实 INSERT（避免 AlreadyExists 短路），actor 为 sender。
    /// 返回 SQL 命令数（应约 7-8 次：advisory_lock + tombstone + messages 读 + state ensure + state lock + CTE + outbox + commit）。
    /// </summary>
    [Benchmark(Description = "Reaction add E2E via Store (full path)")]
    public async Task<int> AddReactionViaStore()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        var emojiSeq = Interlocked.Increment(ref _nextEmojiSeq);
        var emoji = "+e" + emojiSeq;

        await _store.AddAsync(
            messageId: "bench-react-msg",
            actorUserId: 6001,
            actorSessionId: "bench-session",
            emoji: emoji,
            occurredAtMs: 1_700_000_000_000L,
            options: new MessageReactionOptions(),
            CancellationToken.None);

        return NpgsqlSqlCommandCounter.GetCommandCount();
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
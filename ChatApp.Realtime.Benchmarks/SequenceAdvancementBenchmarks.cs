using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 会话序列推进基准：群聊 O(1) 推进、单聊 UPSERT 推进、MarkRead 索引查找。
/// 序列推进方法天然可重复（每次 +1），无需逐迭代重置。
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class SequenceAdvancementBenchmarks
{
    private PostgreSqlContainer _container = null!;
    private RealtimeDatabaseClient _client = null!;
    private RealtimeDatabaseSchema _schema = null!;
    private NpgsqlConnection _connection = null!;

    [GlobalSetup]
    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _schema = new RealtimeDatabaseSchema("realtime_bench_seq");
        _client = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);

        await using var migrateConnection = await _client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        await SeedGroupConversationAsync();
        await SeedDirectMessageAsync();
        await SeedReadIndexMessagesAsync();
    }

    [GlobalCleanup]
    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
        await _client.DisposeAsync();
        await _container.DisposeAsync();
    }

    /// <summary>群消息序列推进：原子递增 conversations.last_sequence 并回写 message 序列。</summary>
    [Benchmark(Description = "Group sequence advance (O(1))")]
    public async Task AdvanceGroupSequence()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await ConversationWriteCommands.TryAdvanceGroupSequenceAsync(
            _connection, tx, _schema,
            conversationId: "bench-group",
            senderUserId: 2001,
            messageId: "bench-msg-group",
            preview: "p",
            receivedAtMs: 1_700_000_000_000L,
            CancellationToken.None);
        await tx.CommitAsync();
    }

    /// <summary>单聊消息序列推进：UPSERT 会话 + 递增 last_sequence + 回写 message 序列。</summary>
    [Benchmark(Description = "Direct sequence advance (UPSERT)")]
    public async Task AdvanceDirectSequence()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await ConversationWriteCommands.TryAdvanceDirectSequenceAsync(
            _connection, tx, _schema,
            conversationId: "bench-direct",
            senderUserId: 3001,
            receiverUserId: 3002,
            messageId: "bench-msg-direct",
            preview: "p",
            receivedAtMs: 1_700_000_000_000L,
            CancellationToken.None);
        await tx.CommitAsync();
    }

    /// <summary>
    /// 极限-2：群消息序列分配（前置）。仅 UPDATE conversations + 发送者 sent_count，
    /// 不回写 messages 行。与 AdvanceGroupSequence 对比可量化"INSERT NULL → UPDATE 回写"
    /// 路径产生的额外 tuple/WAL/索引写入开销。
    /// </summary>
    [Benchmark(Description = "Group sequence allocate (pre-INSERT, no messages UPDATE)")]
    public async Task AllocateGroupSequence()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await ConversationWriteCommands.TryAllocateGroupSequenceAsync(
            _connection, tx, _schema,
            conversationId: "bench-group",
            senderUserId: 2001,
            messageId: "bench-msg-group",
            preview: "p",
            receivedAtMs: 1_700_000_000_000L,
            CancellationToken.None);
        await tx.CommitAsync();
    }

    /// <summary>
    /// 极限-2：单聊消息序列分配（前置）。仅 UPSERT conversations + 发送者 sent_count，
    /// 不回写 messages 行。
    /// </summary>
    [Benchmark(Description = "Direct sequence allocate (pre-INSERT, no messages UPDATE)")]
    public async Task AllocateDirectSequence()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await ConversationWriteCommands.TryAllocateDirectSequenceAsync(
            _connection, tx, _schema,
            conversationId: "bench-direct",
            senderUserId: 3001,
            receiverUserId: 3002,
            messageId: "bench-msg-direct",
            preview: "p",
            receivedAtMs: 1_700_000_000_000L,
            CancellationToken.None);
        await tx.CommitAsync();
    }

    /// <summary>
    /// MarkRead 的 sender_sequence 索引查找（O(log N)）。
    /// 利用 ix_messages_sender_sequence_lookup 替代 O(N) COUNT(*) 扫描。
    /// </summary>
    [Benchmark(Description = "MarkRead sender_sequence lookup (O(log N))")]
    public async Task MarkReadIndexQuery()
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT sender_sequence
             FROM {_schema.MessagesTableSql}
             WHERE conversation_id = @conversation_id
               AND sender_user_id = @user_id
               AND conversation_sequence IS NOT NULL
               AND conversation_sequence <= @target_sequence
             ORDER BY conversation_sequence DESC
             LIMIT 1;
             """,
            _connection);
        command.Parameters.AddWithValue("conversation_id", "bench-read-idx");
        command.Parameters.AddWithValue("user_id", 5001L);
        command.Parameters.AddWithValue("target_sequence", 50L);
        await command.ExecuteScalarAsync();
    }

    private async Task SeedGroupConversationAsync()
    {
        await using var tx = await _connection.BeginTransactionAsync();

        await using (var conv = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_schema.ConversationsTableSql} (
                              conversation_id, type, created_at_ms, updated_at_ms
                          ) VALUES (
                              'bench-group', 2, 1, 1
                          )
                          ON CONFLICT (conversation_id) DO NOTHING;
                          """,
                         _connection, tx))
        {
            await conv.ExecuteNonQueryAsync();
        }

        await using (var member = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_schema.ConversationMembersTableSql} (
                              conversation_id, user_id, peer_user_id, joined_at_ms
                          ) VALUES (
                              'bench-group', 2001, NULL, 1
                          )
                          ON CONFLICT (conversation_id, user_id) DO NOTHING;
                          """,
                         _connection, tx))
        {
            await member.ExecuteNonQueryAsync();
        }

        await using (var msg = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_schema.MessagesTableSql} (
                              message_id, client_message_id, sender_user_id, sender_session_id,
                              receiver_user_id, conversation_id, content, received_at_ms, created_at_ms
                          ) VALUES (
                              'bench-msg-group', 'c-group', 2001, 's', 2002, 'bench-group', 'hello', 1, 1
                          )
                          ON CONFLICT (message_id) DO NOTHING;
                          """,
                         _connection, tx))
        {
            await msg.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }

    private async Task SeedDirectMessageAsync()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await using var msg = new NpgsqlCommand(
            $"""
             INSERT INTO {_schema.MessagesTableSql} (
                 message_id, client_message_id, sender_user_id, sender_session_id,
                 receiver_user_id, conversation_id, content, received_at_ms, created_at_ms
             ) VALUES (
                 'bench-msg-direct', 'c-direct', 3001, 's', 3002, 'bench-direct', 'hi', 1, 1
             )
             ON CONFLICT (message_id) DO NOTHING;
             """,
            _connection, tx);
        await msg.ExecuteNonQueryAsync();
        await tx.CommitAsync();
    }

    /// <summary>为 MarkRead 索引查找预置 100 条带序列号的消息。</summary>
    private async Task SeedReadIndexMessagesAsync()
    {
        await using var tx = await _connection.BeginTransactionAsync();
        await using var msg = new NpgsqlCommand(
            $"""
             INSERT INTO {_schema.MessagesTableSql} (
                 message_id, client_message_id, sender_user_id, sender_session_id,
                 receiver_user_id, conversation_id, content, received_at_ms, created_at_ms,
                 conversation_sequence, sender_sequence
             )
             SELECT 'bench-read-' || g, 'c', 5001, 's', 5002, 'bench-read-idx', 'm', g, g, g, g
             FROM generate_series(1, 100) AS g
             ON CONFLICT (message_id) DO NOTHING;
             """,
            _connection, tx);
        await msg.ExecuteNonQueryAsync();
        await tx.CommitAsync();
    }
}

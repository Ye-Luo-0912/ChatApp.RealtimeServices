using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
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
/// 门禁4：Reply / Mention（含附件、反应批量富化）无 N+1 断言。
/// <para>
/// 历史快照富化必须批量查询：N 条消息（引用 M 个不同 Reply 源 / 附件 / 反应）只允许
/// 恒定次数的 SQL，而不是每消息一次。若 SQL 次数随消息数增长，说明退化为 N+1。
/// 上限取 1（单条 ANY 批量查询）。
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ReplyMentionBenchmarks
{
    private const int SqlBatchEnrich = 1;

    private const int MessageCount = 20;
    private const long ActorUserId = 8001;
    private const long SenderUserId = 8002;
    private const string ConversationId = "bench-reply-conv-001";

    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlConnection? _connection;
    private NpgsqlRealtimeMessageStore? _messageStore;
    private NpgsqlRealtimeAttachmentStore? _attachmentStore;
    private NpgsqlRealtimeReactionStore? _reactionStore;
    private IReadOnlyList<RealtimeHistoryMessage> _replyMessages = [];
    private IReadOnlyList<RealtimeHistoryMessage> _attachmentMessages = [];
    private IReadOnlyList<RealtimeHistoryMessage> _reactionMessages = [];

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
        _schema = new RealtimeDatabaseSchema("realtime_bench_reply");

        await using var migrateConnection = await _dbClient.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        var policy = new PostgresConversationMessageMutationPolicy(
            NullLogger<PostgresConversationMessageMutationPolicy>.Instance);
        _messageStore = new NpgsqlRealtimeMessageStore(
            _dbClient,
            _schema,
            policy,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        _attachmentStore = new NpgsqlRealtimeAttachmentStore(
            _dbClient,
            _schema,
            NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        _reactionStore = new NpgsqlRealtimeReactionStore(
            _dbClient,
            _schema,
            policy);

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

        await using (var conv = new NpgsqlCommand(
                         $"""INSERT INTO {schema.ConversationsTableSql} (conversation_id, type, created_at_ms, updated_at_ms) VALUES (@conv, 1, @now, @now) ON CONFLICT DO NOTHING;""",
                         _connection))
        {
            conv.Parameters.AddWithValue("conv", ConversationId);
            conv.Parameters.AddWithValue("now", nowMs);
            await conv.ExecuteNonQueryAsync();
        }

        // 消息：前 10 条为 Reply 源（其中 5 条已撤回）；后 10 条引用这些源。
        await using (var insert = new NpgsqlCommand(
                         $"""
                          INSERT INTO {schema.MessagesTableSql}
                              ("message_id", "client_message_id", "sender_user_id", "sender_session_id",
                               "receiver_user_id", "content", "received_at_ms", "created_at_ms",
                               "conversation_id", "conversation_sequence", "changed_at_ms",
                               "reply_to_message_id", "recalled_at_ms")
                          SELECT
                              'bench-msg-' || g,
                              'bench-cmsg-' || g,
                              @sender, 'sess',
                              @actor, 'content-' || g,
                              @now, @now - 1000, @conv, g, @now,
                              CASE WHEN g > 10 THEN 'bench-msg-' || (g - 10) ELSE NULL END,
                              CASE WHEN g <= 10 AND g % 2 = 0 THEN @now ELSE NULL END
                          FROM generate_series(1, {MessageCount}) AS g
                          ON CONFLICT DO NOTHING;
                          """,
                         _connection))
        {
            insert.Parameters.AddWithValue("sender", SenderUserId);
            insert.Parameters.AddWithValue("actor", ActorUserId);
            insert.Parameters.AddWithValue("now", nowMs);
            insert.Parameters.AddWithValue("conv", ConversationId);
            await insert.ExecuteNonQueryAsync();
        }

        // 附件：为前 10 条消息各绑定 1 个附件。
        await using (var att = new NpgsqlCommand(
                         $"""
                          INSERT INTO {schema.AttachmentsTableSql}
                              (attachment_id, uploader_user_id, object_key, content_type, size_bytes,
                               status, message_id, conversation_id, created_at_ms, state_version)
                          SELECT 'bench-att-' || g, @sender, 'objects/' || g || '.bin', 'application/octet-stream',
                                 512, @bound, 'bench-msg-' || g, @conv, @now, 0
                          FROM generate_series(1, 10) AS g
                          ON CONFLICT DO NOTHING;
                          """,
                         _connection))
        {
            att.Parameters.AddWithValue("sender", SenderUserId);
            att.Parameters.AddWithValue("bound", (short)AttachmentStatus.Bound);
            att.Parameters.AddWithValue("conv", ConversationId);
            att.Parameters.AddWithValue("now", nowMs);
            await att.ExecuteNonQueryAsync();
        }

        // 反应：为前 10 条消息各添加 1 条反应。
        await using (var react = new NpgsqlCommand(
                         $"""
                          INSERT INTO {schema.MessageReactionsTableSql} (message_id, user_id, emoji, created_at_ms)
                          SELECT 'bench-msg-' || g, @user, '👍', @now
                          FROM generate_series(1, 10) AS g
                          ON CONFLICT DO NOTHING;
                          """,
                         _connection))
        {
            react.Parameters.AddWithValue("user", ActorUserId);
            react.Parameters.AddWithValue("now", nowMs);
            await react.ExecuteNonQueryAsync();
        }

        // 构造内存中的历史消息列表（供三个富化器消费）。
        _replyMessages = Enumerable.Range(1, MessageCount)
            .Select(g => new RealtimeHistoryMessage
            {
                MessageId = $"bench-msg-{g}",
                ClientMessageId = $"bench-cmsg-{g}",
                SenderUserId = SenderUserId,
                ReceiverUserId = ActorUserId,
                ConversationId = ConversationId,
                Content = $"content-{g}",
                ReceivedAtMs = nowMs,
                ChangedAtMs = nowMs,
                ReplyToMessageId = g > 10 ? $"bench-msg-{g - 10}" : null
            })
            .ToArray();

        _attachmentMessages = Enumerable.Range(1, 10)
            .Select(g => new RealtimeHistoryMessage
            {
                MessageId = $"bench-msg-{g}",
                ClientMessageId = $"bench-cmsg-{g}",
                SenderUserId = SenderUserId,
                ReceiverUserId = ActorUserId,
                ConversationId = ConversationId,
                Content = $"content-{g}",
                ReceivedAtMs = nowMs,
                ChangedAtMs = nowMs
            })
            .ToArray();

        _reactionMessages = _attachmentMessages;
    }

    [Benchmark(Description = "Reply enrich: 20 msgs → 10 distinct reply sources (1 SQL)")]
    public async Task<int> EnrichReplySources()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await RealtimeHistoryReplySourceEnricher.EnrichAsync(_messageStore!, _replyMessages);
        return AssertSqlCount(SqlBatchEnrich);
    }

    [Benchmark(Description = "Attachment enrich: 10 msgs → batch ListByMessageIds (1 SQL)")]
    public async Task<int> EnrichAttachments()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await RealtimeHistoryAttachmentEnricher.EnrichAsync(_attachmentStore!, _attachmentMessages);
        return AssertSqlCount(SqlBatchEnrich);
    }

    [Benchmark(Description = "Reaction enrich: 10 msgs → batch ListByMessageIds (1 SQL)")]
    public async Task<int> EnrichReactions()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await RealtimeHistoryReactionEnricher.EnrichAsync(_reactionStore!, _reactionMessages, ActorUserId);
        return AssertSqlCount(SqlBatchEnrich);
    }

    private int AssertSqlCount(int upperBound)
    {
        var count = NpgsqlSqlCommandCounter.GetCommandCount();
        if (count > upperBound)
        {
            throw new InvalidOperationException(
                $"SQL 命令数 {count} 超过门禁上限 {upperBound}。富化必须为单条批量查询，不得退化为 N+1。");
        }
        return count;
    }
}
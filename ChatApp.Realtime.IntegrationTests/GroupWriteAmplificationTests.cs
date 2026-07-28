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
using System.Text.Json;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// Perf-9：群写放大集成测试。
/// <para>
/// 验证 200 人群关键写路径的 Outbox 行数符合 Perf-9 目标：
/// - 群消息发送：≤ 2 行（MessageReceived 广播 + ConversationChanged 广播）；
/// - 群 Reaction：1 行（GroupProjectionDelta 单广播）；
/// - 群标记已读：2 行（1 ConversationRead 广播排除读者 + 1 UnreadCountChanged 自身）；
/// - 群消息编辑：≤ 2 行（MessageEdited 广播 + ConversationChanged 广播）；
/// - 群消息撤回：≤ 2 行（MessageRecalled 广播 + ConversationChanged 广播）。
/// </para>
/// <para>
/// 目标验证 GroupProjectionDelta 协议已将 per-member 事件聚合为单行广播，
/// 消除 O(N) 写放大（N=群成员数）。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class GroupWriteAmplificationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    private const int GroupSize = 200;
    private const long OwnerUserId = 9_200_000_001L;
    private const long ReactorUserId = 9_200_000_002L;
    private const long ReaderUserId = 9_200_000_003L;

    [Fact]
    public async Task GroupSendMessage_ProducesAtMostTwoOutboxRows()
    {
        var (client, schema) = await CreateStoreAsync("rt_group_amplification_send");
        var (groupStore, messageStore, conversationId) = await SeedGroupAsync(client, schema);

        // 清空建群产生的 Outbox 行，确保只计量消息发送操作的写放大
        await ClearAllOutboxAsync(client, schema);

        var message = new RealtimeMessageRecord
        {
            MessageId = "g-send-msg-1",
            ClientMessageId = "g-send-client-1",
            SenderUserId = OwnerUserId,
            SenderSessionId = "s-owner",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "group-broadcast",
            ReceivedAtMs = 1_700_000_000_500
        };
        var evt = new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(
                OwnerUserId, message.ClientMessageId, OwnerUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = OwnerUserId,
            ActorUserId = OwnerUserId,
            MessageId = message.MessageId,
            SessionId = "s-owner",
            OccurredAtMs = message.ReceivedAtMs
        };

        var result = await messageStore.SaveAsync(message, evt);
        Assert.Equal(RealtimeMessagePersistKind.Created, result.Kind);

        var outboxCount = await CountAllOutboxAsync(client, schema);
        Assert.True(
            outboxCount <= 2,
            $"群消息发送 Outbox 行数 {outboxCount} 超过目标 2（Perf-9：广播聚合后应为 ≤ 2）");

        // 验证广播事件携带 target_user_ids 数组（覆盖全部群成员），而非 per-member 单独行
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT event_type, target_user_id, target_user_ids
             FROM {schema.OutboxTableSql}
             ORDER BY event_type
             """,
            connection);
        var rows = new List<(short EventType, long PrimaryTarget, long[]? AllTargets)>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var eventType = reader.GetInt16(0);
            var primary = reader.GetInt64(1);
            long[]? all = reader.IsDBNull(2) ? null : (long[])reader.GetValue(2);
            rows.Add((eventType, primary, all));
        }

        // 至少一行应是带 target_user_ids 的广播（MessageReceived 或 ConversationChanged）
        var broadcastRow = rows.FirstOrDefault(r =>
            r.AllTargets is not null && r.AllTargets.Length == GroupSize);
        Assert.True(
            broadcastRow.AllTargets is not null,
            "未找到覆盖全部群成员的广播行（target_user_ids 长度应为 " + GroupSize + "）");

        _ = groupStore;
    }

    [Fact]
    public async Task GroupReaction_ProducesExactlyOneOutboxRow()
    {
        var (client, schema) = await CreateStoreAsync("rt_group_amplification_reaction");
        var (groupStore, messageStore, conversationId) = await SeedGroupAsync(client, schema);
        var reactionStore = new NpgsqlRealtimeReactionStore(
            client,
            schema,
            new PostgresConversationMessageMutationPolicy(
                NullLogger<PostgresConversationMessageMutationPolicy>.Instance));
        var messageId = await SeedGroupMessageAsync(messageStore, conversationId, "g-react-msg-1");

        // 清空种子消息产生的 Outbox 行，确保只计量 Reaction 操作的写放大
        await ClearAllOutboxAsync(client, schema);

        var result = await reactionStore.AddAsync(
            messageId,
            actorUserId: ReactorUserId,
            actorSessionId: "s-reactor",
            emoji: "👍",
            occurredAtMs: 1_700_000_001_000,
            new MessageReactionOptions());

        Assert.Equal(MessageReactionPersistStatus.Applied, result.Status);

        // 使用 CountAllOutboxAsync 而非按 conversation_id 过滤：部分广播事件不携带 conversation_id
        var outboxCount = await CountAllOutboxAsync(client, schema);
        // 仅 ReactionAdded 广播（1 行），不应有 per-member 事件
        Assert.True(
            outboxCount == 1,
            $"群 Reaction Outbox 行数 {outboxCount} 不等于目标 1（Perf-9：单广播聚合）");

        _ = groupStore;
    }

    [Fact]
    public async Task GroupMarkRead_ProducesExactlyTwoOutboxRows()
    {
        var (client, schema) = await CreateStoreAsync("rt_group_amplification_markread");
        var (groupStore, messageStore, conversationId) = await SeedGroupAsync(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var messageId = await SeedGroupMessageAsync(messageStore, conversationId, "g-read-msg-1");

        // 清空种子消息产生的 Outbox 行，确保只计量 MarkRead 操作的写放大
        await ClearAllOutboxAsync(client, schema);

        var result = await conversationStore.AdvanceReadCursorAsync(
            userId: ReaderUserId,
            conversationId,
            readAtMs: 1_700_000_001_500,
            readMessageId: messageId);

        Assert.True(result.Found);

        // 1 ConversationRead 广播（排除读者）+ 1 UnreadCountChanged（读者自身）
        // 使用 CountAllOutboxAsync：UnreadCountChanged 行可能不携带 conversation_id
        var outboxCount = await CountAllOutboxAsync(client, schema);
        Assert.True(
            outboxCount == 2,
            $"群标记已读 Outbox 行数 {outboxCount} 不等于目标 2（Perf-9：1 广播 + 1 自身）");

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT event_type, target_user_id, target_user_ids
             FROM {schema.OutboxTableSql}
             ORDER BY event_type
             """,
            connection);
        var rows = new List<(short EventType, long PrimaryTarget, long[]? AllTargets)>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((
                reader.GetInt16(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : (long[])reader.GetValue(2)));
        }

        // ConversationRead 广播行：target_user_ids 应排除读者 ReaderUserId
        var readBroadcast = rows.SingleOrDefault(r =>
            r.EventType == (short)RealtimeEventType.ConversationRead);
        Assert.NotNull(readBroadcast.AllTargets);
        Assert.Contains(OwnerUserId, readBroadcast.AllTargets!);
        Assert.DoesNotContain(ReaderUserId, readBroadcast.AllTargets!);

        // UnreadCountChanged 行：target_user_id 应为读者本人
        var unreadRow = rows.SingleOrDefault(r =>
            r.EventType == (short)RealtimeEventType.UnreadCountChanged);
        Assert.Equal(ReaderUserId, unreadRow.PrimaryTarget);

        _ = groupStore;
    }

    [Fact]
    public async Task GroupEditMessage_ProducesAtMostTwoOutboxRows()
    {
        var (client, schema) = await CreateStoreAsync("rt_group_amplification_edit");
        var (groupStore, messageStore, conversationId) = await SeedGroupAsync(client, schema);
        var messageId = await SeedGroupMessageAsync(messageStore, conversationId, "g-edit-msg-1");

        // 清空种子消息产生的 Outbox 行，确保只计量 Edit 操作的写放大
        await ClearAllOutboxAsync(client, schema);

        var result = await messageStore.ApplyEditAsync(
            requestId: "req-edit-1",
            messageId,
            senderUserId: OwnerUserId,
            senderSessionId: "s-owner",
            content: "edited-content",
            editedAtMs: 1_700_000_002_000,
            maxAgeMs: 60_000);

        Assert.Equal(MessageEditPersistStatus.Applied, result.Status);

        // 使用 CountAllOutboxAsync：部分广播事件可能不携带 conversation_id
        var outboxCount = await CountAllOutboxAsync(client, schema);
        Assert.True(
            outboxCount <= 2,
            $"群消息编辑 Outbox 行数 {outboxCount} 超过目标 2（Perf-9：广播聚合后应为 ≤ 2）");

        _ = groupStore;
    }

    [Fact]
    public async Task GroupRecallMessage_ProducesAtMostTwoOutboxRows()
    {
        var (client, schema) = await CreateStoreAsync("rt_group_amplification_recall");
        var (groupStore, messageStore, conversationId) = await SeedGroupAsync(client, schema);
        var messageId = await SeedGroupMessageAsync(messageStore, conversationId, "g-recall-msg-1");

        // 清空种子消息产生的 Outbox 行，确保只计量 Recall 操作的写放大
        await ClearAllOutboxAsync(client, schema);

        var result = await messageStore.ApplyRecallAsync(
            requestId: "req-recall-1",
            messageId,
            senderUserId: OwnerUserId,
            senderSessionId: "s-owner",
            recalledAtMs: 1_700_000_002_500,
            maxAgeMs: 60_000);

        Assert.Equal(MessageRecallPersistStatus.Applied, result.Status);

        // 使用 CountAllOutboxAsync：部分广播事件可能不携带 conversation_id
        var outboxCount = await CountAllOutboxAsync(client, schema);
        Assert.True(
            outboxCount <= 2,
            $"群消息撤回 Outbox 行数 {outboxCount} 超过目标 2（Perf-9：广播聚合后应为 ≤ 2）");

        _ = groupStore;
    }

    private async Task<(NpgsqlRealtimeGroupStore GroupStore, NpgsqlRealtimeMessageStore MessageStore, string ConversationId)> SeedGroupAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema)
    {
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            new PostgresConversationMessageMutationPolicy(
                NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        var conversationId = ConversationId.CreateGroup();
        var memberIds = Enumerable.Range(0, GroupSize)
            .Select(i => (long)(OwnerUserId + i))
            .ToArray();

        await groupStore.CreateGroupAsync(
            requestId: "req-create-amplification",
            creatorUserId: OwnerUserId,
            conversationId,
            title: "AmplificationTest",
            memberUserIds: memberIds,
            actorSessionId: "s-owner",
            occurredAtMs: 1_700_000_000_000);

        return (groupStore, messageStore, conversationId);
    }

    private static async Task<string> SeedGroupMessageAsync(
        NpgsqlRealtimeMessageStore messageStore,
        string conversationId,
        string messageId)
    {
        var message = new RealtimeMessageRecord
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = OwnerUserId,
            SenderSessionId = "s-owner",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "seed",
            ReceivedAtMs = 1_700_000_000_400
        };
        var evt = new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(
                OwnerUserId, message.ClientMessageId, OwnerUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = OwnerUserId,
            ActorUserId = OwnerUserId,
            MessageId = messageId,
            SessionId = "s-owner",
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = messageId,
                    ClientMessageId = message.ClientMessageId,
                    SenderUserId = OwnerUserId,
                    SenderSessionId = "s-owner",
                    ReceiverUserId = 0,
                    ConversationId = conversationId,
                    Content = "seed",
                    ReceivedAtMs = 1_700_000_000_400
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = 1_700_000_000_400
        };

        var result = await messageStore.SaveAsync(message, evt);
        Assert.Equal(RealtimeMessagePersistKind.Created, result.Kind);
        return messageId;
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateStoreAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }

    /// <summary>
    /// 清空当前 schema 的全部 Outbox 行。每个测试使用独立 schema，清空安全。
    /// 用于在种子消息写入后、被测操作执行前重置计数基线。
    /// </summary>
    private static async Task ClearAllOutboxAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"DELETE FROM {schema.OutboxTableSql}",
            connection);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// 统计当前 schema 的全部 Outbox 行数（不按 conversation_id 过滤）。
    /// 部分广播事件（如 UnreadCountChanged）可能不携带 conversation_id，
    /// 按 conversation_id 过滤会导致漏计，因此写放大量化使用全表计数。
    /// </summary>
    private static async Task<long> CountAllOutboxAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*)::bigint FROM {schema.OutboxTableSql}",
            connection);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}

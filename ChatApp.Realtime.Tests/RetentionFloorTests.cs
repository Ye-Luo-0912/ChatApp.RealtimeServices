using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// 三-3：retention_floor_sequence 推进与列表未读公式集成测试。
/// <para>
/// 验证 NpgsqlRealtimeMessageRetentionStore 删除消息后会推进 retention_floor_sequence，
/// 且列表查询的未读公式使用 GREATEST(last_read_sequence, retention_floor_sequence)
/// 把已删除区间从未读数中扣除。
/// </para>
/// </summary>
public sealed class RetentionFloorTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task PurgeBatch_AdvancesRetentionFloorSequence_BelowDeletedMessages()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_retention_floor_advance");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var retentionStore = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);

        var conversationId = ConversationId.CreateDirect(30001, 30002);
        // 三条消息，按服务端到达顺序获得 conversation_sequence = 1,2,3
        await messageStore.SaveAsync(
            CreateMessage("m-floor-1", 30001, 30002, conversationId, "old-1", 1_000),
            CreateMessageReceivedEvent("evt-floor-1", 30002, "m-floor-1", 1_000));
        await messageStore.SaveAsync(
            CreateMessage("m-floor-2", 30001, 30002, conversationId, "old-2", 2_000),
            CreateMessageReceivedEvent("evt-floor-2", 30002, "m-floor-2", 2_000));
        await messageStore.SaveAsync(
            CreateMessage("m-floor-3", 30001, 30002, conversationId, "new-1", 100_000),
            CreateMessageReceivedEvent("evt-floor-3", 30002, "m-floor-3", 100_000));

        // 删除前 floor = 0
        Assert.Equal(0, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // 删除 received_at_ms < 10_000 的两条旧消息
        var result = await retentionStore.TryPurgeBatchAsync(cutoffReceivedAtMs: 10_000, batchSize: 100);
        Assert.Equal(2, result.DeletedCount);

        // floor 推进到被删除消息的最大 conversation_sequence（=2）
        Assert.Equal(2, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // 仅剩 m-floor-3
        var remaining = await ListMessageIdsAsync(client, schema, conversationId);
        Assert.Equal(["m-floor-3"], remaining);
    }

    [Fact]
    public async Task ListUnread_FloorClampsDeletedRange_AfterRetentionPurge()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_retention_floor_unread");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var retentionStore = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);

        var conversationId = ConversationId.CreateDirect(31001, 31002);
        // 接收者 31002 从未读，发送者 31001 发送 3 条消息：seq=1,2,3
        await messageStore.SaveAsync(
            CreateMessage("m-uno-1", 31001, 31002, conversationId, "old-1", 1_000),
            CreateMessageReceivedEvent("evt-uno-1", 31002, "m-uno-1", 1_000));
        await messageStore.SaveAsync(
            CreateMessage("m-uno-2", 31001, 31002, conversationId, "old-2", 2_000),
            CreateMessageReceivedEvent("evt-uno-2", 31002, "m-uno-2", 2_000));
        await messageStore.SaveAsync(
            CreateMessage("m-uno-3", 31001, 31002, conversationId, "new-1", 100_000),
            CreateMessageReceivedEvent("evt-uno-3", 31002, "m-uno-3", 100_000));

        // 删除前未读 = 3（last_sequence=3, last_read_sequence=0, retention_floor=0）
        var listBefore = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-before-purge",
                UserId = 31002,
                Limit = 10
            });
        Assert.True(listBefore.Succeeded);
        var itemBefore = Assert.Single(listBefore.Items);
        Assert.Equal(3, itemBefore.UnreadCount);

        // Retention 删除前两条（seq=1,2），floor 推进到 2
        var result = await retentionStore.TryPurgeBatchAsync(cutoffReceivedAtMs: 10_000, batchSize: 100);
        Assert.Equal(2, result.DeletedCount);
        Assert.Equal(2, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // 删除后未读 = 1：max(0, 2) = 2，3 - 2 - (0 - 0) = 1
        var listAfter = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-after-purge",
                UserId = 31002,
                Limit = 10
            });
        Assert.True(listAfter.Succeeded);
        var itemAfter = Assert.Single(listAfter.Items);
        Assert.Equal(1, itemAfter.UnreadCount);
        // tip 仍为最后一条存活消息
        Assert.Equal("m-uno-3", itemAfter.LastMessageId);
    }

    [Fact]
    public async Task ListUnread_FloorBelowLastRead_LastReadStillAuthoritative()
    {
        // 当 last_read_sequence > retention_floor_sequence 时，未读公式仍以 last_read_sequence 为准，
        // 验证 max(last_read_sequence, retention_floor_sequence) 语义。
        var (client, schema) = await CreateDatabaseAsync("realtime_retention_floor_read_wins");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());
        var retentionStore = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);

        var conversationId = ConversationId.CreateDirect(32001, 32002);
        await messageStore.SaveAsync(
            CreateMessage("m-rw-1", 32001, 32002, conversationId, "first", 1_000),
            CreateMessageReceivedEvent("evt-rw-1", 32002, "m-rw-1", 1_000));
        await messageStore.SaveAsync(
            CreateMessage("m-rw-2", 32001, 32002, conversationId, "second", 2_000),
            CreateMessageReceivedEvent("evt-rw-2", 32002, "m-rw-2", 2_000));
        await messageStore.SaveAsync(
            CreateMessage("m-rw-3", 32001, 32002, conversationId, "third", 3_000),
            CreateMessageReceivedEvent("evt-rw-3", 32002, "m-rw-3", 3_000));
        await messageStore.SaveAsync(
            CreateMessage("m-rw-4", 32001, 32002, conversationId, "fourth", 4_000),
            CreateMessageReceivedEvent("evt-rw-4", 32002, "m-rw-4", 4_000));

        // 接收者读到 m-rw-3（seq=3）
        var read = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-rw-3",
                UserId = 32002,
                ConversationId = conversationId,
                ReadMessageId = "m-rw-3"
            });
        Assert.True(read.Succeeded);
        Assert.Equal(1, read.UnreadCount); // 仅 m-rw-4 未读

        // Retention 删除 m-rw-1（seq=1），floor 推进到 1（< last_read_sequence=3）
        var purge = await retentionStore.TryPurgeBatchAsync(cutoffReceivedAtMs: 1_500, batchSize: 100);
        Assert.Equal(1, purge.DeletedCount);
        Assert.Equal(1, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // 未读仍为 1：max(3, 1) = 3，4 - 3 - (0 - 0) = 1
        var list = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-read-wins",
                UserId = 32002,
                Limit = 10
            });
        Assert.True(list.Succeeded);
        var item = Assert.Single(list.Items);
        Assert.Equal(1, item.UnreadCount);
        Assert.Equal("m-rw-4", item.LastMessageId);
    }

    [Fact]
    public async Task RetentionFloor_UpdatesSentCountAtRetentionFloor_AfterPurge()
    {
        // P0-2：验证 Retention 推进 floor 后，发送者的 sent_count_at_retention_floor 正确更新。
        var (client, schema) = await CreateDatabaseAsync("realtime_retention_sent_floor");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var retentionStore = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);

        var conversationId = ConversationId.CreateDirect(33001, 33002);
        // 发送者 33001 发送 3 条消息：seq=1,2,3，sender_sequence=1,2,3
        await messageStore.SaveAsync(
            CreateMessage("m-sf-1", 33001, 33002, conversationId, "old-1", 1_000),
            CreateMessageReceivedEvent("evt-sf-1", 33002, "m-sf-1", 1_000));
        await messageStore.SaveAsync(
            CreateMessage("m-sf-2", 33001, 33002, conversationId, "old-2", 2_000),
            CreateMessageReceivedEvent("evt-sf-2", 33002, "m-sf-2", 2_000));
        await messageStore.SaveAsync(
            CreateMessage("m-sf-3", 33001, 33002, conversationId, "new-1", 100_000),
            CreateMessageReceivedEvent("evt-sf-3", 33002, "m-sf-3", 100_000));

        // 删除前：发送者 sent_count_at_retention_floor = sent_count_at_read（回填值，Migration045）
        Assert.Equal(0, await GetSentCountAtRetentionFloorAsync(client, schema, conversationId, 33001));

        // 删除 seq=1,2，floor 推进到 2
        await retentionStore.TryPurgeBatchAsync(cutoffReceivedAtMs: 10_000, batchSize: 100);
        Assert.Equal(2, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // 发送者在 floor=2 处的累计发送数 = 2（floor 之后第一条消息 sender_sequence=3，3-1=2）
        Assert.Equal(2, await GetSentCountAtRetentionFloorAsync(client, schema, conversationId, 33001));
        // 接收者从未发送，sent_count_at_retention_floor = 0
        Assert.Equal(0, await GetSentCountAtRetentionFloorAsync(client, schema, conversationId, 33002));
    }

    [Fact]
    public async Task ListUnread_FloorBeyondReadCursor_UsesSentCountAtRetentionFloor()
    {
        // P0-2：当 retention_floor_sequence > last_read_sequence 时，未读公式必须使用
        // sent_count_at_retention_floor 而非 sent_count_at_read，否则已删除的自发送消息
        // 会被多扣减，导致未读数偏低。
        var (client, schema) = await CreateDatabaseAsync("realtime_retention_floor_beyond_read");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());
        var retentionStore = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);

        var conversationId = ConversationId.CreateDirect(34001, 34002);
        // A(34001) 发送 seq=1,2,3，B(34002) 发送 seq=4,5,6
        await messageStore.SaveAsync(
            CreateMessage("m-fb-1", 34001, 34002, conversationId, "a-1", 1_000),
            CreateMessageReceivedEvent("evt-fb-1", 34002, "m-fb-1", 1_000));
        await messageStore.SaveAsync(
            CreateMessage("m-fb-2", 34001, 34002, conversationId, "a-2", 2_000),
            CreateMessageReceivedEvent("evt-fb-2", 34002, "m-fb-2", 2_000));
        await messageStore.SaveAsync(
            CreateMessage("m-fb-3", 34001, 34002, conversationId, "a-3", 3_000),
            CreateMessageReceivedEvent("evt-fb-3", 34002, "m-fb-3", 3_000));
        await messageStore.SaveAsync(
            CreateMessage("m-fb-4", 34002, 34001, conversationId, "b-1", 4_000),
            CreateMessageReceivedEvent("evt-fb-4", 34001, "m-fb-4", 4_000));
        await messageStore.SaveAsync(
            CreateMessage("m-fb-5", 34002, 34001, conversationId, "b-2", 5_000),
            CreateMessageReceivedEvent("evt-fb-5", 34001, "m-fb-5", 5_000));
        await messageStore.SaveAsync(
            CreateMessage("m-fb-6", 34002, 34001, conversationId, "b-3", 6_000),
            CreateMessageReceivedEvent("evt-fb-6", 34001, "m-fb-6", 6_000));

        // A 读到 seq=1，sent_count_at_read=1（A 在 seq=1 前发了 1 条）
        var read = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-fb-1",
                UserId = 34001,
                ConversationId = conversationId,
                ReadMessageId = "m-fb-1"
            });
        Assert.True(read.Succeeded);
        // 未读 = 6 - 1 - (3 - 1) = 3（seq 2,3 是 A 自己的，seq 4,5,6 来自 B，但 seq 2,3 仍计入未读
        // 因为 sent_count - sent_count_at_read = 2 扣除了 A 自发送的 seq 2,3）
        Assert.Equal(3, read.UnreadCount);

        // Retention 删除 seq=1,2,3（A 的全部消息），floor=3
        var purge = await retentionStore.TryPurgeBatchAsync(cutoffReceivedAtMs: 3_500, batchSize: 100);
        Assert.Equal(3, purge.DeletedCount);
        Assert.Equal(3, await GetRetentionFloorSequenceAsync(client, schema, conversationId));

        // A 的 sent_count_at_retention_floor=3（无 floor 后存活消息 → sent_count=3）
        Assert.Equal(3, await GetSentCountAtRetentionFloorAsync(client, schema, conversationId, 34001));

        // A 的未读 = 6 - GREATEST(1, 3) - (3 - 3) = 6 - 3 - 0 = 3（seq 4,5,6 来自 B）
        var listA = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-fb-a",
                UserId = 34001,
                Limit = 10
            });
        Assert.True(listA.Succeeded);
        var itemA = Assert.Single(listA.Items);
        Assert.Equal(3, itemA.UnreadCount);

        // B 的未读 = 6 - GREATEST(0, 3) - (3 - 0) = 6 - 3 - 3 = 0（seq 4,5,6 是 B 自己发的）
        var listB = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-fb-b",
                UserId = 34002,
                Limit = 10
            });
        Assert.True(listB.Succeeded);
        var itemB = Assert.Single(listB.Items);
        Assert.Equal(0, itemB.UnreadCount);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateDatabaseAsync(
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

    private static async Task<long> GetRetentionFloorSequenceAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string conversationId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT retention_floor_sequence
             FROM {schema.ConversationsTableSql}
             WHERE conversation_id = @cid;
             """,
            connection);
        cmd.Parameters.AddWithValue("cid", conversationId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<long> GetSentCountAtRetentionFloorAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long userId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT sent_count_at_retention_floor
             FROM {schema.ConversationMembersTableSql}
             WHERE conversation_id = @cid AND user_id = @uid;
             """,
            connection);
        cmd.Parameters.AddWithValue("cid", conversationId);
        cmd.Parameters.AddWithValue("uid", userId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<List<string>> ListMessageIdsAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string conversationId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT message_id FROM {schema.MessagesTableSql} WHERE conversation_id = @cid ORDER BY message_id",
            connection);
        cmd.Parameters.AddWithValue("cid", conversationId);
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetString(0));
        return ids;
    }

    private static RealtimeMessageRecord CreateMessage(
        string messageId,
        long sender,
        long receiver,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "session-retention",
            ReceiverUserId = receiver,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };

    private static RealtimeEvent CreateMessageReceivedEvent(
        string eventId,
        long targetUserId,
        string messageId,
        long occurredAtMs) =>
        new()
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = targetUserId,
            MessageId = messageId,
            OccurredAtMs = occurredAtMs
        };
}

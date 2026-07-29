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

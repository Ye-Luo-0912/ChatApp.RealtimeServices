using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using System.Text.Json;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class GroupConversationReadTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task GroupMarkRead_AdvancesWatermark_AndFansOutConversationRead()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_mark_read");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            outboxSignal);

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            501,
            conversationId,
            "Reads",
            [502, 503],
            "s1",
            1_700_000_000_000);

        await messageStore.SaveAsync(
            CreateGroupMessage("g-msg-1", 501, conversationId, "hello", 1_700_000_000_100),
            CreateGroupReceivedEvent("g-msg-1", 501, conversationId, "hello", 1_700_000_000_100));

        var before = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-before",
                UserId = 502,
                Limit = 10
            });
        Assert.True(before.Succeeded);
        var itemBefore = Assert.Single(before.Items);
        Assert.True(itemBefore.UnreadCount >= 1);

        var markRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-group-1",
                UserId = 502,
                ConversationId = conversationId
            });

        Assert.True(markRead.Succeeded);
        Assert.True(markRead.Changed);
        Assert.Equal(0, markRead.UnreadCount);
        Assert.Equal("g-msg-1", markRead.LastReadMessageId);
        Assert.Equal(1_700_000_000_100, markRead.LastReadAtMs);
        Assert.Equal(1, outboxSignal.Notifications);

        var after = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-after",
                UserId = 502,
                Limit = 10
            });
        Assert.True(after.Succeeded);
        var itemAfter = Assert.Single(after.Items);
        Assert.Equal(0, itemAfter.UnreadCount);
        Assert.Equal("g-msg-1", itemAfter.LastReadMessageId);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var unreadCmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
               AND target_user_id = 502
             """,
            connection);
        unreadCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.UnreadCountChanged);
        Assert.True(Convert.ToInt32(await unreadCmd.ExecuteScalarAsync()) >= 1);

        await using var readCmd = new NpgsqlCommand(
            $"""
             SELECT target_user_id, payload_json
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             ORDER BY target_user_id
             """,
            connection);
        readCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.ConversationRead);
        var readTargets = new List<long>();
        await using var readReader = await readCmd.ExecuteReaderAsync();
        while (await readReader.ReadAsync())
        {
            readTargets.Add(readReader.GetInt64(0));
            var evt = RealtimeWireSerializer.DeserializeEvent(readReader.GetString(1));
            Assert.NotNull(evt);
            var payload = RealtimeWireSerializer.DeserializeConversationRead(evt.PayloadJson!);
            Assert.NotNull(payload);
            Assert.Equal(conversationId, payload.ConversationId);
            Assert.Equal(502, payload.ReaderUserId);
            Assert.Equal("g-msg-1", payload.LastReadMessageId);
            Assert.Equal(1_700_000_000_100, payload.LastReadAtMs);
        }

        Assert.Equal([501L, 503L], readTargets);
        Assert.DoesNotContain(502L, readTargets);
    }

    [Fact]
    public async Task GroupMarkRead_NonMember_Fails()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_mark_read_gate");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            601,
            conversationId,
            "Gate",
            [602],
            "s1",
            1_700_000_000_000);

        var rejected = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-outsider",
                UserId = 9999,
                ConversationId = conversationId
            });

        Assert.False(rejected.Succeeded);
        Assert.Equal("not_found", rejected.ErrorCode);
    }

    [Fact]
    public async Task DirectMarkRead_StillWorks_AndNotifiesPeerWithConversationRead()
    {
        var (client, schema) = await CreateStoreAsync("realtime_dm_mark_read_peer");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var conversationId = ConversationId.CreateDirect(701, 702);
        await messageStore.SaveAsync(
            CreateDirectMessage("dm-1", 701, 702, conversationId, "hi", 100),
            CreateDirectReceivedEvent("dm-1", 701, 702, conversationId, "hi", 100));

        var markRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "dm-read-1",
                UserId = 702,
                ConversationId = conversationId
            });

        Assert.True(markRead.Succeeded);
        Assert.True(markRead.Changed);
        Assert.Equal(0, markRead.UnreadCount);
        Assert.Equal("dm-1", markRead.LastReadMessageId);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var unreadCmd = new NpgsqlCommand(
            $"""
             SELECT target_user_id
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             ORDER BY target_user_id
             """,
            connection);
        unreadCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.UnreadCountChanged);
        var unreadTargets = new List<long>();
        await using (var reader = await unreadCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                unreadTargets.Add(reader.GetInt64(0));
        }

        // New-message unread for 702, plus mark-read UnreadCountChanged for 702.
        Assert.Contains(702L, unreadTargets);

        await using var readCmd = new NpgsqlCommand(
            $"""
             SELECT target_user_id, payload_json
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             """,
            connection);
        readCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.ConversationRead);
        await using var readReader = await readCmd.ExecuteReaderAsync();
        Assert.True(await readReader.ReadAsync());
        Assert.Equal(701L, readReader.GetInt64(0));
        var evt = RealtimeWireSerializer.DeserializeEvent(readReader.GetString(1));
        Assert.NotNull(evt);
        var payload = RealtimeWireSerializer.DeserializeConversationRead(evt.PayloadJson!);
        Assert.NotNull(payload);
        Assert.Equal(702, payload.ReaderUserId);
        Assert.Equal("dm-1", payload.LastReadMessageId);
        Assert.False(await readReader.ReadAsync());
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

    private static RealtimeMessageRecord CreateGroupMessage(
        string messageId,
        long sender,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "s1",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };

    private static RealtimeEvent CreateGroupReceivedEvent(
        string messageId,
        long sender,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateMessageReceivedEventId(sender, $"client-{messageId}", sender),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = sender,
            ActorUserId = sender,
            MessageId = messageId,
            SessionId = "s1",
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = messageId,
                    ClientMessageId = $"client-{messageId}",
                    SenderUserId = sender,
                    SenderSessionId = "s1",
                    ReceiverUserId = 0,
                    ConversationId = conversationId,
                    Content = content,
                    ReceivedAtMs = receivedAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = receivedAtMs
        };

    private static RealtimeMessageRecord CreateDirectMessage(
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
            SenderSessionId = "s1",
            ReceiverUserId = receiver,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };

    private static RealtimeEvent CreateDirectReceivedEvent(
        string messageId,
        long sender,
        long receiver,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateMessageReceivedEventId(sender, $"client-{messageId}"),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = receiver,
            ActorUserId = sender,
            MessageId = messageId,
            SessionId = "s1",
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = messageId,
                    ClientMessageId = $"client-{messageId}",
                    SenderUserId = sender,
                    SenderSessionId = "s1",
                    ReceiverUserId = receiver,
                    ConversationId = conversationId,
                    Content = content,
                    ReceivedAtMs = receivedAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = receivedAtMs
        };
}

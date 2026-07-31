using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Routing;
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
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            outboxSignal);

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            "req-read-create",
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

        // 极限-3：群已读走会话级广播（audience_kind=Conversation + ExcludeUserId=读者），
        // 不再物化 target_user_ids 数组。Publisher 通过 IConversationGatewayDirectory 投递，
        // Gateway 跳过 ExcludeUserId 的会话。
        await using var readCmd = new NpgsqlCommand(
            $"""
             SELECT target_user_id, target_user_ids, audience_kind, conversation_id, exclude_user_id,
                   COALESCE(payload_json, convert_from(payload_utf8, 'UTF8'))
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             """,
            connection);
        readCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.ConversationRead);
        var readRows = new List<(long PrimaryTarget, long[]? AllTargets, short AudienceKind, string? ConversationId, long? ExcludeUserId)>();
        await using var readReader = await readCmd.ExecuteReaderAsync();
        while (await readReader.ReadAsync())
        {
            var primaryTarget = readReader.GetInt64(0);
            long[]? allTargets = readReader.IsDBNull(1) ? null : (long[])readReader.GetValue(1);
            var audienceKind = readReader.IsDBNull(2) ? (short)0 : readReader.GetInt16(2);
            var convId = readReader.IsDBNull(3) ? null : readReader.GetString(3);
            long? excludeUserId = readReader.IsDBNull(4) ? null : readReader.GetInt64(4);
            readRows.Add((primaryTarget, allTargets, audienceKind, convId, excludeUserId));
            var rawJson = readReader.GetString(5);
            var evt = RealtimeWireSerializer.DeserializeEvent(rawJson);
            Assert.NotNull(evt);
            // 调试：验证 wire payload 中的 ExcludeUserId
            Assert.True(evt.ExcludeUserId == 502L, $"ExcludeUserId={evt.ExcludeUserId}, json contains 'exclude': {rawJson.Contains("exclude", StringComparison.OrdinalIgnoreCase)}, json={rawJson}");
            var payload = RealtimeWireSerializer.DeserializeConversationRead(evt.PayloadJson!);
            Assert.NotNull(payload);
            Assert.Equal(conversationId, payload.ConversationId);
            Assert.Equal(502, payload.ReaderUserId);
            Assert.Equal("g-msg-1", payload.LastReadMessageId);
            Assert.Equal(1_700_000_000_100, payload.LastReadAtMs);
        }

        // 单行会话级广播事件：audience_kind=Conversation、conversation_id=群会话、
        // target_user_ids=NULL、exclude_user_id=读者 502。
        var broadcastRow = Assert.Single(readRows);
        Assert.Equal((short)AudienceKind.Conversation, broadcastRow.AudienceKind);
        Assert.Equal(conversationId, broadcastRow.ConversationId);
        Assert.Null(broadcastRow.AllTargets);
        Assert.Equal(502L, broadcastRow.ExcludeUserId);
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
            "req-gate-create",
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
    public async Task GroupMarkRead_Debounce_SuppressesBroadcastWithinWindow()
    {
        // 极限-3：群 MarkRead debounce——读者在时间窗内连续 MarkRead，
        // 仅写自身未读变更，不向其余成员广播 ConversationRead，避免大群 O(N²) 投递。
        var (client, schema) = await CreateStoreAsync("realtime_group_mark_read_debounce");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            "req-debounce-create",
            801,
            conversationId,
            "Debounce",
            [802, 803],
            "s1",
            1_700_000_000_000);

        // 第一条消息
        await messageStore.SaveAsync(
            CreateGroupMessage("g-db-1", 801, conversationId, "first", 1_700_000_000_100),
            CreateGroupReceivedEvent("g-db-1", 801, conversationId, "first", 1_700_000_000_100));

        // 首次 MarkRead：LastReadBroadcastAtMs IS NULL → 广播
        var firstRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-db-1",
                UserId = 802,
                ConversationId = conversationId
            });
        Assert.True(firstRead.Succeeded);
        Assert.True(firstRead.Changed);

        // 清空 Outbox，只计量第二次 MarkRead
        await using (var clearConn = await client.GetDataSource().OpenConnectionAsync())
        await using (var clearCmd = new NpgsqlCommand(
            $"DELETE FROM {schema.OutboxTableSql}", clearConn))
        {
            await clearCmd.ExecuteNonQueryAsync();
        }

        // 第二条消息（序列推进 1，远小于阈值 10）
        await messageStore.SaveAsync(
            CreateGroupMessage("g-db-2", 801, conversationId, "second", 1_700_000_000_200),
            CreateGroupReceivedEvent("g-db-2", 801, conversationId, "second", 1_700_000_000_200));

        // 第二次 MarkRead：时间窗内 + 序列推进 < 10 → 抑制广播
        var secondRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-db-2",
                UserId = 802,
                ConversationId = conversationId
            });
        Assert.True(secondRead.Succeeded);
        Assert.True(secondRead.Changed);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();

        // 抑制期间不应有 ConversationRead 广播
        await using var readCmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             """,
            connection);
        readCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.ConversationRead);
        var readCount = (int)(await readCmd.ExecuteScalarAsync())!;
        Assert.Equal(0, readCount);

        // 读者自身未读变更仍应写入（即时反馈）
        await using var unreadCmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
               AND target_user_id = 802
             """,
            connection);
        unreadCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.UnreadCountChanged);
        var unreadCount = (int)(await unreadCmd.ExecuteScalarAsync())!;
        Assert.True(unreadCount >= 1, "debounce 抑制期间仍应写读者自身未读变更");
    }

    [Fact]
    public async Task GroupMarkRead_SequenceThreshold_BreaksDebounce()
    {
        // 极限-3：即使时间窗内，读水位推进超过序列阈值时仍应广播，
        // 保证其余成员能在合理延迟内感知读者进度。
        var (client, schema) = await CreateStoreAsync("realtime_group_mark_read_threshold");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            "req-threshold-create",
            901,
            conversationId,
            "Threshold",
            [902, 903],
            "s1",
            1_700_000_000_000);

        // 首条消息 + 首次 MarkRead（建立广播水位）
        await messageStore.SaveAsync(
            CreateGroupMessage("g-th-1", 901, conversationId, "first", 1_700_000_000_100),
            CreateGroupReceivedEvent("g-th-1", 901, conversationId, "first", 1_700_000_000_100));
        var firstRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-th-1",
                UserId = 902,
                ConversationId = conversationId
            });
        Assert.True(firstRead.Succeeded);

        // 清空 Outbox
        await using (var clearConn = await client.GetDataSource().OpenConnectionAsync())
        await using (var clearCmd = new NpgsqlCommand(
            $"DELETE FROM {schema.OutboxTableSql}", clearConn))
        {
            await clearCmd.ExecuteNonQueryAsync();
        }

        // 发送超过序列阈值的消息（GroupReadBroadcastSequenceThreshold = 10）
        for (var i = 2; i <= 12; i++)
        {
            await messageStore.SaveAsync(
                CreateGroupMessage($"g-th-{i}", 901, conversationId, $"msg-{i}", 1_700_000_000_100 + i * 100),
                CreateGroupReceivedEvent($"g-th-{i}", 901, conversationId, $"msg-{i}", 1_700_000_000_100 + i * 100));
        }

        // 第二次 MarkRead：序列推进 >= 10 → 突破 debounce，应广播
        var secondRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-th-2",
                UserId = 902,
                ConversationId = conversationId
            });
        Assert.True(secondRead.Succeeded);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var readCmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             """,
            connection);
        readCmd.Parameters.AddWithValue("type", (short)RealtimeEventType.ConversationRead);
        var readCount = (int)(await readCmd.ExecuteScalarAsync())!;
        Assert.True(readCount >= 1, "序列推进超过阈值时应突破 debounce 广播 ConversationRead");
    }

    [Fact]
    public async Task DirectMarkRead_StillWorks_AndNotifiesPeerWithConversationRead()
    {
        var (client, schema) = await CreateStoreAsync("realtime_dm_mark_read_peer");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
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
             SELECT target_user_id, COALESCE(payload_json, convert_from(payload_utf8, 'UTF8'))
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
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(sender, $"client-{messageId}", sender),
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
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(sender, $"client-{messageId}"),
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

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class ConversationListAndUnreadTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task QueryList_MarkRead_UpdatesUnreadCount()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_list_unread");
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

        var conversationId = ConversationId.CreateDirect(2001, 2002);
        await messageStore.SaveAsync(
            CreateMessage("msg-1", 2001, 2002, conversationId, "first", 100),
            CreateMessageReceivedEvent("evt-1", 2002, "msg-1", 100));
        await messageStore.SaveAsync(
            CreateMessage("msg-2", 2001, 2002, conversationId, "second", 200),
            CreateMessageReceivedEvent("evt-2", 2002, "msg-2", 200));

        var listBefore = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-1",
                UserId = 2002,
                Limit = 20
            });

        Assert.True(listBefore.Succeeded);
        var itemBefore = Assert.Single(listBefore.Items);
        Assert.Equal(conversationId, itemBefore.ConversationId);
        Assert.Equal(2, itemBefore.UnreadCount);
        Assert.Equal("msg-2", itemBefore.LastMessageId);

        var markRead = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-1",
                UserId = 2002,
                ConversationId = conversationId
            });

        Assert.True(markRead.Succeeded);
        Assert.True(markRead.Changed);
        Assert.Equal(0, markRead.UnreadCount);
        Assert.Equal("msg-2", markRead.LastReadMessageId);
        Assert.Equal(200, markRead.LastReadAtMs);
        Assert.Equal(1, outboxSignal.Notifications);

        var listAfter = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-2",
                UserId = 2002,
                Limit = 20
            });

        Assert.True(listAfter.Succeeded);
        var itemAfter = Assert.Single(listAfter.Items);
        Assert.Equal(0, itemAfter.UnreadCount);
        Assert.Equal("msg-2", itemAfter.LastReadMessageId);
        Assert.Equal(200, itemAfter.LastReadAtMs);
    }

    [Fact]
    public async Task SenderConversationList_HasZeroUnread()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_sender_unread");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);

        var conversationId = ConversationId.CreateDirect(3001, 3002);
        await messageStore.SaveAsync(
            CreateMessage("msg-a", 3001, 3002, conversationId, "hello", 50),
            CreateMessageReceivedEvent("evt-a", 3002, "msg-a", 50));

        var page = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-sender",
                UserId = 3001,
                Limit = 10
            });

        Assert.True(page.Succeeded);
        var item = Assert.Single(page.Items);
        Assert.Equal(0, item.UnreadCount);
    }

    [Fact]
    public async Task Pin_SortsPinnedConversationFirst()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_pin_sort");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            outboxSignal);

        var olderId = ConversationId.CreateDirect(4001, 4002);
        var newerId = ConversationId.CreateDirect(4001, 4003);
        await messageStore.SaveAsync(
            CreateMessage("msg-old", 4002, 4001, olderId, "older", 100),
            CreateMessageReceivedEvent("evt-old", 4001, "msg-old", 100));
        await messageStore.SaveAsync(
            CreateMessage("msg-new", 4003, 4001, newerId, "newer", 200),
            CreateMessageReceivedEvent("evt-new", 4001, "msg-new", 200));

        var beforePin = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-before-pin",
                UserId = 4001,
                Limit = 20
            });
        Assert.True(beforePin.Succeeded);
        Assert.Equal(2, beforePin.Items.Count);
        Assert.Equal(newerId, beforePin.Items[0].ConversationId);
        Assert.Equal(olderId, beforePin.Items[1].ConversationId);

        var pin = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "pin-1",
                UserId = 4001,
                ConversationId = olderId,
                Pinned = true
            });
        Assert.True(pin.Succeeded);
        Assert.True(pin.Changed);
        Assert.True(pin.IsPinned);
        Assert.Equal(1, outboxSignal.Notifications);

        var afterPin = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-after-pin",
                UserId = 4001,
                Limit = 20
            });
        Assert.True(afterPin.Succeeded);
        Assert.Equal(2, afterPin.Items.Count);
        Assert.Equal(olderId, afterPin.Items[0].ConversationId);
        Assert.True(afterPin.Items[0].IsPinned);
        Assert.Equal(newerId, afterPin.Items[1].ConversationId);
        Assert.False(afterPin.Items[1].IsPinned);
    }

    [Fact]
    public async Task Repin_AlreadyPinned_DoesNotRefreshPinnedAtOrOutbox()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_repin_noop");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            outboxSignal);

        var conversationId = ConversationId.CreateDirect(4101, 4102);
        await messageStore.SaveAsync(
            CreateMessage("msg-repin", 4102, 4101, conversationId, "hi", 100),
            CreateMessageReceivedEvent("evt-repin", 4101, "msg-repin", 100));

        var first = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "pin-first",
                UserId = 4101,
                ConversationId = conversationId,
                Pinned = true
            });
        Assert.True(first.Succeeded);
        Assert.True(first.Changed);
        Assert.Equal(1, outboxSignal.Notifications);

        long? pinnedAtMs;
        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        await using (var cmd = new NpgsqlCommand(
                           $"""
                            SELECT pinned_at_ms
                            FROM {schema.ConversationMembersTableSql}
                            WHERE conversation_id = @cid AND user_id = 4101;
                            """,
                           connection))
        {
            cmd.Parameters.AddWithValue("cid", conversationId);
            pinnedAtMs = (long?)await cmd.ExecuteScalarAsync();
        }

        Assert.NotNull(pinnedAtMs);
        await Task.Delay(5);

        var second = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "pin-again",
                UserId = 4101,
                ConversationId = conversationId,
                Pinned = true
            });
        Assert.True(second.Succeeded);
        Assert.False(second.Changed);
        Assert.Equal(1, outboxSignal.Notifications);

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        await using (var cmd = new NpgsqlCommand(
                           $"""
                            SELECT pinned_at_ms
                            FROM {schema.ConversationMembersTableSql}
                            WHERE conversation_id = @cid AND user_id = 4101;
                            """,
                           connection))
        {
            cmd.Parameters.AddWithValue("cid", conversationId);
            var again = (long?)await cmd.ExecuteScalarAsync();
            Assert.Equal(pinnedAtMs, again);
        }
    }

    [Fact]
    public async Task MutePrefs_PersistAndUnmuteClearsUntil()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_mute_prefs");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            outboxSignal);

        var conversationId = ConversationId.CreateDirect(5001, 5002);
        await messageStore.SaveAsync(
            CreateMessage("msg-m", 5001, 5002, conversationId, "hi", 50),
            CreateMessageReceivedEvent("evt-m", 5002, "msg-m", 50));

        const long untilMs = 1_900_000_000_000L;
        var mute = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "mute-1",
                UserId = 5002,
                ConversationId = conversationId,
                Muted = true,
                MutedUntilMs = untilMs
            });
        Assert.True(mute.Succeeded);
        Assert.True(mute.Changed);
        Assert.True(mute.IsMuted);
        Assert.Equal(untilMs, mute.MutedUntilMs);

        var mutedList = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-muted",
                UserId = 5002,
                Limit = 10
            });
        Assert.True(mutedList.Succeeded);
        var mutedItem = Assert.Single(mutedList.Items);
        Assert.True(mutedItem.IsMuted);
        Assert.Equal(untilMs, mutedItem.MutedUntilMs);
        Assert.False(mutedItem.IsPinned);

        var unmute = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "unmute-1",
                UserId = 5002,
                ConversationId = conversationId,
                Muted = false
            });
        Assert.True(unmute.Succeeded);
        Assert.True(unmute.Changed);
        Assert.False(unmute.IsMuted);
        Assert.Null(unmute.MutedUntilMs);

        var unmutedList = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-unmuted",
                UserId = 5002,
                Limit = 10
            });
        Assert.True(unmutedList.Succeeded);
        var unmutedItem = Assert.Single(unmutedList.Items);
        Assert.False(unmutedItem.IsMuted);
        Assert.Null(unmutedItem.MutedUntilMs);
        Assert.Equal(2, outboxSignal.Notifications);
    }

    [Fact]
    public async Task Pin_Pagination_DoesNotSkipUnpinnedConversations()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_pin_page");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var pinnedId = ConversationId.CreateDirect(6001, 6002);
        var newerUnpinnedId = ConversationId.CreateDirect(6001, 6003);
        var olderUnpinnedId = ConversationId.CreateDirect(6001, 6004);
        await messageStore.SaveAsync(
            CreateMessage("msg-pin", 6002, 6001, pinnedId, "pinned", 100),
            CreateMessageReceivedEvent("evt-pin", 6001, "msg-pin", 100));
        await messageStore.SaveAsync(
            CreateMessage("msg-new", 6003, 6001, newerUnpinnedId, "newer", 300),
            CreateMessageReceivedEvent("evt-new", 6001, "msg-new", 300));
        await messageStore.SaveAsync(
            CreateMessage("msg-old", 6004, 6001, olderUnpinnedId, "older", 200),
            CreateMessageReceivedEvent("evt-old", 6001, "msg-old", 200));

        var pin = await prefsProcessor.ProcessAsync(
            new ConversationSetPrefsCommand
            {
                RequestId = "pin-page",
                UserId = 6001,
                ConversationId = pinnedId,
                Pinned = true
            });
        Assert.True(pin.Succeeded);

        var page1 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "page-1",
                UserId = 6001,
                Limit = 1
            });
        Assert.True(page1.Succeeded);
        Assert.Equal(pinnedId, Assert.Single(page1.Items).ConversationId);
        Assert.True(page1.HasMore);
        Assert.NotNull(page1.NextCursor);
        Assert.True(page1.NextCursor.IsPinned);

        var page2 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "page-2",
                UserId = 6001,
                BeforeIsPinned = page1.NextCursor.IsPinned,
                BeforePinnedAtMs = page1.NextCursor.PinnedAtMs,
                BeforeLastMessageAtMs = page1.NextCursor.LastMessageAtMs,
                BeforeConversationId = page1.NextCursor.ConversationId,
                Limit = 10
            });
        Assert.True(page2.Succeeded);
        Assert.Equal(2, page2.Items.Count);
        Assert.Equal(newerUnpinnedId, page2.Items[0].ConversationId);
        Assert.Equal(olderUnpinnedId, page2.Items[1].ConversationId);
        Assert.All(page2.Items, item => Assert.False(item.IsPinned));
    }

    [Fact]
    public async Task ListQuery_RejectsPartialCursor()
    {
        var listProcessor = new DefaultConversationListQueryProcessor(
            new ThrowingConversationStore());

        var onlyId = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "partial-id",
                UserId = 1,
                BeforeConversationId = "dm:1:2",
                Limit = 10
            });
        Assert.False(onlyId.Succeeded);
        Assert.Equal("invalid_cursor", onlyId.ErrorCode);

        var onlyPinned = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "partial-pinned",
                UserId = 1,
                BeforeIsPinned = false,
                Limit = 10
            });
        Assert.False(onlyPinned.Succeeded);
        Assert.Equal("invalid_cursor", onlyPinned.ErrorCode);

        var onlyLastAt = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "partial-last-at",
                UserId = 1,
                BeforeLastMessageAtMs = 100,
                Limit = 10
            });
        Assert.False(onlyLastAt.Succeeded);
        Assert.Equal("invalid_cursor", onlyLastAt.ErrorCode);

        var missingId = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "partial-no-id",
                UserId = 1,
                BeforeIsPinned = true,
                BeforePinnedAtMs = 50,
                BeforeLastMessageAtMs = 100,
                Limit = 10
            });
        Assert.False(missingId.Succeeded);
        Assert.Equal("invalid_cursor", missingId.ErrorCode);
    }

    [Fact]
    public async Task MarkRead_IgnoresUnknownOrForeignMessageAndUsesDbTimestamp()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_read_clamp");
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

        var conversationId = ConversationId.CreateDirect(7001, 7002);
        var otherConversationId = ConversationId.CreateDirect(7001, 7003);
        await messageStore.SaveAsync(
            CreateMessage("msg-1", 7001, 7002, conversationId, "first", 100),
            CreateMessageReceivedEvent("evt-1", 7002, "msg-1", 100));
        await messageStore.SaveAsync(
            CreateMessage("msg-2", 7001, 7002, conversationId, "second", 200),
            CreateMessageReceivedEvent("evt-2", 7002, "msg-2", 200));
        await messageStore.SaveAsync(
            CreateMessage("msg-other", 7001, 7003, otherConversationId, "other", 9_000),
            CreateMessageReceivedEvent("evt-other", 7003, "msg-other", 9_000));

        var unknown = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-unknown",
                UserId = 7002,
                ConversationId = conversationId,
                ReadAtMs = 999,
                ReadMessageId = "does-not-exist"
            });
        Assert.True(unknown.Succeeded);
        Assert.False(unknown.Changed);
        Assert.Equal(2, unknown.UnreadCount);

        // 其他会话的消息 Id 不能推进当前会话已读游标。
        var foreign = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-foreign",
                UserId = 7002,
                ConversationId = conversationId,
                ReadAtMs = 9_000,
                ReadMessageId = "msg-other"
            });
        Assert.True(foreign.Succeeded);
        Assert.False(foreign.Changed);
        Assert.Equal(2, foreign.UnreadCount);

        // 客户端伪造未来时间戳时，以库内 received_at_ms 为准。
        var forgedTime = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-forged-time",
                UserId = 7002,
                ConversationId = conversationId,
                ReadAtMs = 9_999_999,
                ReadMessageId = "msg-1"
            });
        Assert.True(forgedTime.Succeeded);
        Assert.True(forgedTime.Changed);
        Assert.Equal(1, forgedTime.UnreadCount);
        Assert.Equal("msg-1", forgedTime.LastReadMessageId);
        Assert.Equal(100, forgedTime.LastReadAtMs);

        // 人为写入一条晚于 tip 的消息但不更新会话摘要，游标应被钳到 tip。
        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        await using (var insert = new Npgsql.NpgsqlCommand(
                           $"""
                            INSERT INTO {schema.MessagesTableSql} (
                                message_id, client_message_id, sender_user_id, sender_session_id,
                                receiver_user_id, conversation_id, content, content_fingerprint,
                                received_at_ms, created_at_ms
                            ) VALUES (
                                'msg-ahead', 'client-ahead', 7001, 'session-1',
                                7002, @conversation_id, 'ahead', 'fp',
                                500, 500
                            );
                            """,
                           connection))
        {
            insert.Parameters.AddWithValue("conversation_id", conversationId);
            await insert.ExecuteNonQueryAsync();
        }

        var clamped = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-clamp-tip",
                UserId = 7002,
                ConversationId = conversationId,
                ReadAtMs = 500,
                ReadMessageId = "msg-ahead"
            });
        Assert.True(clamped.Succeeded);
        Assert.True(clamped.Changed);
        // Perf-1：序列模型下游标钳到 tip（msg-2, seq=2）。
        // 直接 INSERT 绕过 Store 的孤儿消息无 conversation_sequence，不计入序列未读。
        // 生产路径所有消息经 Store 写入，均会被分配序列号。
        Assert.Equal(0, clamped.UnreadCount);
        Assert.Equal("msg-2", clamped.LastReadMessageId);
        Assert.Equal(200, clamped.LastReadAtMs);

        var list = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-after-clamp",
                UserId = 7002,
                Limit = 10
            });
        Assert.Equal(0, Assert.Single(list.Items).UnreadCount);
        Assert.Equal("msg-2", list.Items[0].LastReadMessageId);
        Assert.Equal(200, list.Items[0].LastReadAtMs);
    }

    [Fact]
    public async Task UnreadCycle_EmitsDistinctOutboxEventIds()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_unread_cycle");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var markReadProcessor = new DefaultConversationMarkReadProcessor(
            conversationStore,
            new RecordingRealtimeOutboxSignal());

        var conversationId = ConversationId.CreateDirect(9001, 9002);
        await messageStore.SaveAsync(
            CreateMessage("msg-1", 9001, 9002, conversationId, "first", 100),
            CreateMessageReceivedEvent("evt-1", 9002, "msg-1", 100));

        var read1 = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-cycle-1",
                UserId = 9002,
                ConversationId = conversationId
            });
        Assert.True(read1.Succeeded);
        Assert.Equal(0, read1.UnreadCount);

        await messageStore.SaveAsync(
            CreateMessage("msg-2", 9001, 9002, conversationId, "second", 200),
            CreateMessageReceivedEvent("evt-2", 9002, "msg-2", 200));

        // P0-1：消息写入不再发射 UnreadCountChanged 事件，未读数由序列公式派生。
        // 仅 MarkRead 发射 UnreadCountChanged；两次 MarkRead 命中不同消息，事件 ID 必须不同。
        var read2 = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "read-cycle-2",
                UserId = 9002,
                ConversationId = conversationId
            });
        Assert.True(read2.Succeeded);
        Assert.Equal(0, read2.UnreadCount);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            $"""
             SELECT event_id, COALESCE(payload_json, convert_from(payload_utf8, 'UTF8'))
             FROM {schema.OutboxTableSql}
             WHERE event_type = @event_type
               AND target_user_id = 9002
             ORDER BY created_at_ms, event_id
             """,
            connection);
        command.Parameters.AddWithValue("event_type", (short)RealtimeEventType.UnreadCountChanged);

        var eventIds = new List<string>();
        var unreadCounts = new List<int>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            eventIds.Add(reader.GetString(0));
            var evt = RealtimeWireSerializer.DeserializeEvent(reader.GetString(1));
            Assert.NotNull(evt);
            var payload = RealtimeWireSerializer.DeserializeUnreadCountChanged(evt.PayloadJson!);
            Assert.NotNull(payload);
            unreadCounts.Add(payload.UnreadCount);
        }

        Assert.Equal([0, 0], unreadCounts);
        Assert.Equal(2, eventIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task OutOfOrderMessage_StillIncrementsUnread()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_ooo_unread");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var listProcessor = new DefaultConversationListQueryProcessor(conversationStore);

        var conversationId = ConversationId.CreateDirect(8001, 8002);
        await messageStore.SaveAsync(
            CreateMessage("msg-new", 8001, 8002, conversationId, "newer", 200),
            CreateMessageReceivedEvent("evt-new", 8002, "msg-new", 200));
        await messageStore.SaveAsync(
            CreateMessage("msg-old", 8001, 8002, conversationId, "older", 100),
            CreateMessageReceivedEvent("evt-old", 8002, "msg-old", 100));

        var page = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "list-ooo",
                UserId = 8002,
                Limit = 10
            });
        Assert.True(page.Succeeded);
        var item = Assert.Single(page.Items);
        Assert.Equal("msg-new", item.LastMessageId);
        Assert.Equal(2, item.UnreadCount);
    }

    /// <summary>
    /// 二-2：单聊序列模型未读公式集成测试。
    /// <para>
    /// 验证 last_sequence / last_read_sequence / retention_floor_sequence 三者协同：
    /// 1. 发送 4 条消息后未读 = 4（last_sequence=4, last_read_sequence=0, floor=0）
    /// 2. MarkRead 到第 2 条后未读 = 2（last_read_sequence=2）
    /// 3. 再发 2 条后未读 = 4（last_sequence=6, last_read_sequence=2）
    /// 4. MarkRead 到 tip 后未读 = 0
    /// </para>
    /// </summary>
    [Fact]
    public async Task DirectMessage_SequenceModel_UnreadCorrectAcrossReadAndNewMessages()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_seq_unread_cycle");
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

        var conversationId = ConversationId.CreateDirect(11_001, 11_002);
        // 发送 4 条消息
        for (var i = 1; i <= 4; i++)
        {
            await messageStore.SaveAsync(
                CreateMessage($"seq-msg-{i}", 11_001, 11_002, conversationId, $"msg-{i}", i * 100),
                CreateMessageReceivedEvent($"seq-evt-{i}", 11_002, $"seq-msg-{i}", i * 100));
        }

        // 未读 = 4
        var list1 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "seq-list-1",
                UserId = 11_002,
                Limit = 10
            });
        Assert.True(list1.Succeeded);
        Assert.Equal(4, Assert.Single(list1.Items).UnreadCount);
        Assert.Equal("seq-msg-4", Assert.Single(list1.Items).LastMessageId);

        // 读到第 2 条
        var read1 = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "seq-read-1",
                UserId = 11_002,
                ConversationId = conversationId,
                ReadMessageId = "seq-msg-2"
            });
        Assert.True(read1.Succeeded);
        Assert.Equal(2, read1.UnreadCount);

        // 列表未读 = 2
        var list2 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "seq-list-2",
                UserId = 11_002,
                Limit = 10
            });
        Assert.True(list2.Succeeded);
        Assert.Equal(2, Assert.Single(list2.Items).UnreadCount);

        // 再发 2 条
        for (var i = 5; i <= 6; i++)
        {
            await messageStore.SaveAsync(
                CreateMessage($"seq-msg-{i}", 11_001, 11_002, conversationId, $"msg-{i}", i * 100),
                CreateMessageReceivedEvent($"seq-evt-{i}", 11_002, $"seq-msg-{i}", i * 100));
        }

        // 未读 = 4（last_sequence=6, last_read_sequence=2）
        var list3 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "seq-list-3",
                UserId = 11_002,
                Limit = 10
            });
        Assert.True(list3.Succeeded);
        Assert.Equal(4, Assert.Single(list3.Items).UnreadCount);
        Assert.Equal("seq-msg-6", Assert.Single(list3.Items).LastMessageId);

        // 读到 tip
        var read2 = await markReadProcessor.ProcessAsync(
            new ConversationMarkReadCommand
            {
                RequestId = "seq-read-2",
                UserId = 11_002,
                ConversationId = conversationId
            });
        Assert.True(read2.Succeeded);
        Assert.Equal(0, read2.UnreadCount);

        // 列表未读 = 0
        var list4 = await listProcessor.ProcessAsync(
            new ConversationListQuery
            {
                RequestId = "seq-list-4",
                UserId = 11_002,
                Limit = 10
            });
        Assert.True(list4.Succeeded);
        Assert.Equal(0, Assert.Single(list4.Items).UnreadCount);
    }

    /// <summary>
    /// 二-3：归档快照列表门禁测试。
    /// <para>
    /// 验证 <see cref="IRealtimeConversationStore.QueryArchivedListAsync"/> 在用户离群后
    /// 返回离群时刻的消息快照（left_message_*）而非群当前 tip。
    /// 否则群在用户离开后继续活跃时，归档用户会看到离群后的最新预览。
    /// </para>
    /// <para>
    /// 验证步骤：
    /// 1. 创建群，发送 2 条消息
    /// 2. 成员离群（快照记录 tip = msg-before-leave）
    /// 3. 离群后再发 1 条消息（tip 推进到 msg-after-leave）
    /// 4. 归档列表返回 left_message（msg-before-leave），未读=0
    /// </para>
    /// </summary>
    [Fact]
    public async Task ArchiveSnapshot_LeavesGroup_ShowsSnapshotMessageNotCurrentTip()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_conv_archive_snapshot");
        var groupStore = new NpgsqlRealtimeGroupStore(
            client,
            schema,
            membershipPeriodStore: new NpgsqlMembershipPeriodStore(client, schema));
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);

        var conversationId = ConversationId.CreateGroup();
        const long owner = 12_001;
        const long member = 12_002;
        await groupStore.CreateGroupAsync(
            "req-archive-create",
            owner,
            conversationId,
            "ArchiveSnapshot",
            [member],
            "s-owner",
            1_700_000_000_000);

        // 发送 2 条消息
        await SendGroupMessageAsync(messageStore, conversationId, owner, "msg-before-1", 1_700_000_000_100);
        await SendGroupMessageAsync(messageStore, conversationId, owner, "msg-before-2", 1_700_000_000_200);

        // 成员离群（快照记录 tip = msg-before-2）
        var leave = await groupStore.LeaveAsync(
            "req-archive-leave",
            member,
            conversationId,
            "s-member",
            1_700_000_001_000);
        Assert.True(leave.Succeeded);

        // 离群后再发 1 条消息（tip 推进到 msg-after-leave）
        await SendGroupMessageAsync(messageStore, conversationId, owner, "msg-after-leave", 1_700_000_002_000);

        // 归档列表：应返回离群快照（msg-before-2），而非当前 tip（msg-after-leave）
        var archived = await conversationStore.QueryArchivedListAsync(
            userId: member,
            beforeIsPinned: null,
            beforePinnedAtMs: null,
            beforeLastMessageAtMs: null,
            beforeConversationId: null,
            take: 10);

        var archivedItem = Assert.Single(archived);
        Assert.Equal(conversationId, archivedItem.ConversationId);
        Assert.Equal(ConversationType.Group, archivedItem.Type);
        // 关键断言：归档列表显示离群时刻的消息，而非群当前 tip
        Assert.Equal("g-msg-before-2", archivedItem.LastMessageId);
        Assert.Equal("msg-before-2", archivedItem.LastMessagePreview);
        Assert.Equal(1_700_000_000_200, archivedItem.LastMessageAtMs);
        // 离群后未读永远为 0
        Assert.Equal(0, archivedItem.UnreadCount);
    }

    private static async Task SendGroupMessageAsync(
        NpgsqlRealtimeMessageStore messageStore,
        string conversationId,
        long senderUserId,
        string content,
        long receivedAtMs)
    {
        var message = new RealtimeMessageRecord
        {
            MessageId = $"g-{content}",
            ClientMessageId = $"g-client-{content}",
            SenderUserId = senderUserId,
            SenderSessionId = $"s{senderUserId}",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };
        var template = new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(senderUserId, message.ClientMessageId, 1),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = message.MessageId,
            SessionId = message.SenderSessionId!,
            OccurredAtMs = receivedAtMs
        };
        var persisted = await messageStore.SaveAsync(message, template);
        Assert.Equal(RealtimeMessagePersistKind.Created, persisted.Kind);
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
            SenderSessionId = "session-1",
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

    private sealed class ThrowingConversationStore : IRealtimeConversationStore
    {
        public Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
            long userId,
            bool? beforeIsPinned,
            long? beforePinnedAtMs,
            long? beforeLastMessageAtMs,
            string? beforeConversationId,
            int take,
            CancellationToken ct = default) =>
            throw new InvalidOperationException("partial cursor must fail validation before store.");

        public Task<IReadOnlyList<ConversationListItem>> QueryArchivedListAsync(
            long userId,
            bool? beforeIsPinned,
            long? beforePinnedAtMs,
            long? beforeLastMessageAtMs,
            string? beforeConversationId,
            int take,
            CancellationToken ct = default) =>
            throw new InvalidOperationException("partial cursor must fail validation before store.");

        public Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
            long userId,
            string conversationId,
            long? readAtMs,
            string? readMessageId,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
            long userId,
            string conversationId,
            bool? pinned,
            bool? muted,
            long? mutedUntilMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}

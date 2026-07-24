using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Core.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ChatApp.Realtime.Tests;

public sealed class SyncBootstrapQueryProcessorTests
{
    [Fact]
    public async Task ProcessAsync_ReturnsConversationListAndCatchUpForUnread()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 2,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            new NoopDeviceCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-1",
            UserId = 42,
            ListLimit = 10,
            HistoryLimitPerConversation = 20,
            MaxConversationsWithHistory = 5
        });

        Assert.True(page.Succeeded);
        var conversation = Assert.Single(page.Conversations);
        Assert.Equal(conversationId, conversation.ConversationId);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal(conversationId, catchUp.ConversationId);
        Assert.Equal(2, catchUp.Items.Count);
        Assert.False(catchUp.HasMore);
        Assert.True(historyStore.ConversationQueryCalled);
        Assert.False(historyStore.AfterQueryCalled);
    }

    [Fact]
    public async Task ProcessAsync_UsesAfterQueryWhenWatermarkProvided()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            new NoopDeviceCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-2",
            UserId = 42,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                }
            ]
        });

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Single(catchUp.Items);
        Assert.Equal("msg-2", catchUp.Items[0].MessageId);
        Assert.True(historyStore.AfterQueryCalled);
        Assert.Equal(100, historyStore.AfterReceivedAtMs);
        Assert.Equal("msg-1", historyStore.AfterMessageId);
    }

    [Fact]
    public async Task ProcessAsync_ResetsFutureWatermark()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            deviceStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-future",
            UserId = 42,
            DeviceIdHash = 7,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 9_999_999,
                    AfterMessageId = "future-msg"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.False(historyStore.AfterQueryCalled);
        Assert.Empty(page.CatchUps);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(conversationId, reset.ConversationId);
        Assert.Equal(SyncCursorResetReason.MessageNotFound, reset.Reason);
        // 空 catch-up / reset 不得推进游标：否则未来水位钳 tip 会永久跳过未投递历史。
        Assert.Empty(deviceStore.Upserted);
    }

    [Fact]
    public async Task ProcessAsync_ResetsRandomInvalidWatermark()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            new NoopDeviceCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-random",
            UserId = 42,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 50,
                    AfterMessageId = "does-not-exist"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.False(historyStore.AfterQueryCalled);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(conversationId, reset.ConversationId);
        Assert.Equal(SyncCursorResetReason.MessageNotFound, reset.Reason);
        Assert.Equal("msg-2", reset.TipMessageId);
        Assert.Equal(200, reset.TipReceivedAtMs);
    }

    [Fact]
    public async Task ProcessAsync_EmptyConversation_SkipsPersistForMissingTip()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = null,
                LastMessageAtMs = null
            }
        ]);
        var historyStore = new CapturingHistoryStore(conversationId, []);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            deviceStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-empty",
            UserId = 42,
            DeviceIdHash = 11,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "ghost"
                }
            ]
        });

        Assert.True(page.Succeeded);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(SyncCursorResetReason.MessageNotFound, reset.Reason);
        Assert.Null(reset.TipMessageId);
        Assert.Null(reset.TipReceivedAtMs);
        Assert.Empty(deviceStore.Upserted);
    }

    [Fact]
    public async Task ProcessAsync_EmitsMembershipLostReset_ForNonMemberWatermark()
    {
        var memberId = "dm:42:43";
        var foreignId = "dm:99:100";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = memberId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            memberId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = memberId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = memberId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ])
        {
            MemberConversationIds = new HashSet<string>(StringComparer.Ordinal) { memberId }
        };
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            new NoopDeviceCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-mixed",
            UserId = 42,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = memberId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                },
                new ConversationSyncWatermark
                {
                    ConversationId = foreignId,
                    AfterReceivedAtMs = 50,
                    AfterMessageId = "secret-1"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.Contains(foreignId, historyStore.FilteredConversationIds);
        Assert.DoesNotContain(foreignId, historyStore.QueriedConversationIds);
        Assert.Contains(memberId, historyStore.QueriedConversationIds);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal(memberId, catchUp.ConversationId);
        Assert.Equal("msg-2", Assert.Single(catchUp.Items).MessageId);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(foreignId, reset.ConversationId);
        Assert.Equal(SyncCursorResetReason.MembershipLost, reset.Reason);
        Assert.Null(reset.TipMessageId);
    }

    [Fact]
    public async Task ProcessAsync_LoadsServerDeviceCursorsWhenWatermarksOmitted()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore(
        [
            new DeviceSyncCursor
            {
                ConversationId = conversationId,
                AfterReceivedAtMs = 100,
                AfterMessageId = "msg-1"
            }
        ]);
        var processor = new DefaultSyncBootstrapQueryProcessor(
            conversationStore,
            historyStore,
            deviceStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-device",
            UserId = 42,
            DeviceIdHash = 99,
            MaxConversationsWithHistory = 5
        });

        Assert.True(page.Succeeded);
        Assert.True(historyStore.AfterQueryCalled);
        Assert.Equal(100, historyStore.AfterReceivedAtMs);
        Assert.Equal("msg-1", historyStore.AfterMessageId);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal("msg-2", Assert.Single(catchUp.Items).MessageId);
        var persisted = Assert.Single(deviceStore.Upserted);
        Assert.Equal("msg-2", persisted.AfterMessageId);
        Assert.Equal(200, persisted.AfterReceivedAtMs);
    }

    [Fact]
    public async Task ProcessAsync_RejectsInvalidRequestId()
    {
        var processor = new DefaultSyncBootstrapQueryProcessor(
            new CapturingConversationStore([]),
            new CapturingHistoryStore("dm:1:2", []),
            new NoopDeviceCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = string.Empty,
            UserId = 42
        });

        Assert.False(page.Succeeded);
        Assert.Equal("invalid_request_id", page.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_DoesNotPersistCursor_WhenClientWatermarkIsInFuture()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(conversationId, []);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(conversationStore, historyStore, deviceStore);

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-future",
            UserId = 42,
            DeviceIdHash = 7,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 999,
                    AfterMessageId = "msg-future"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.Empty(page.CatchUps);
        Assert.Empty(deviceStore.Upserted);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(conversationId, reset.ConversationId);
        Assert.Equal(SyncCursorResetReason.MessageNotFound, reset.Reason);
        Assert.Equal(200, reset.TipReceivedAtMs);
        Assert.Equal("msg-2", reset.TipMessageId);
        Assert.False(historyStore.AfterQueryCalled);
    }

    [Fact]
    public async Task ProcessAsync_DoesNotPersistCursor_WhenClientMessageIdIsUnknown()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(conversationStore, historyStore, deviceStore);

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-random-id",
            UserId = 42,
            DeviceIdHash = 8,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 150,
                    AfterMessageId = "msg-deleted"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.Empty(deviceStore.Upserted);
        Assert.False(historyStore.AfterQueryCalled);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(SyncCursorResetReason.MessageNotFound, reset.Reason);
        Assert.Equal("msg-2", reset.TipMessageId);
    }

    [Fact]
    public async Task ProcessAsync_PersistsLastReturnedMessage_NotRawWatermark()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(conversationStore, historyStore, deviceStore);

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-returned",
            UserId = 42,
            DeviceIdHash = 9,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                }
            ]
        });

        Assert.True(page.Succeeded);
        var persisted = Assert.Single(deviceStore.Upserted);
        Assert.Equal("msg-2", persisted.AfterMessageId);
        Assert.Equal(200, persisted.AfterReceivedAtMs);
        Assert.NotEqual("msg-1", persisted.AfterMessageId);
    }

    [Fact]
    public async Task ProcessAsync_EmitsAheadOfTipReset_WhenWatermarkMessageIsBeyondTip()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-1",
                LastMessageAtMs = 100
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(conversationStore, historyStore, deviceStore);

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-ahead",
            UserId = 42,
            DeviceIdHash = 7,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 200,
                    AfterMessageId = "msg-2"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.False(historyStore.AfterQueryCalled);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(conversationId, reset.ConversationId);
        Assert.Equal(SyncCursorResetReason.AheadOfTip, reset.Reason);
        Assert.Equal("msg-1", reset.TipMessageId);
        Assert.Equal(100, reset.TipReceivedAtMs);
        Assert.Empty(deviceStore.Upserted);
    }

    [Fact]
    public async Task ProcessAsync_EmitsGapTooLargeReset_WhenConfiguredAndLagExceedsThreshold()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 10_000
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 10_000
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(
            conversationStore,
            historyStore,
            deviceStore,
            new SyncBootstrapOptions { MaxCatchUpGapMs = 1_000 });

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-gap",
            UserId = 42,
            DeviceIdHash = 12,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.False(historyStore.AfterQueryCalled);
        var reset = Assert.Single(page.ResetsRequired);
        Assert.Equal(SyncCursorResetReason.GapTooLarge, reset.Reason);
        Assert.Equal("msg-2", reset.TipMessageId);
        Assert.Equal(10_000, reset.TipReceivedAtMs);
        Assert.Empty(deviceStore.Upserted);
    }

    [Fact]
    public async Task ProcessAsync_ValidWatermark_RemainsIncremental_WithoutReset()
    {
        var conversationId = "dm:42:43";
        var conversationStore = new CapturingConversationStore(
        [
            new ConversationListItem
            {
                ConversationId = conversationId,
                UnreadCount = 0,
                LastMessageId = "msg-2",
                LastMessageAtMs = 200
            }
        ]);
        var historyStore = new CapturingHistoryStore(
            conversationId,
            [
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-1",
                    ClientMessageId = "client-1",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "hello",
                    ReceivedAtMs = 100
                },
                new RealtimeHistoryMessage
                {
                    MessageId = "msg-2",
                    ClientMessageId = "client-2",
                    SenderUserId = 43,
                    ReceiverUserId = 42,
                    ConversationId = conversationId,
                    Content = "world",
                    ReceivedAtMs = 200
                }
            ]);
        var deviceStore = new CapturingDeviceCursorStore([]);
        var processor = CreateProcessor(conversationStore, historyStore, deviceStore);

        var page = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-valid",
            UserId = 42,
            DeviceIdHash = 13,
            MaxConversationsWithHistory = 5,
            Watermarks =
            [
                new ConversationSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                }
            ]
        });

        Assert.True(page.Succeeded);
        Assert.Empty(page.ResetsRequired);
        Assert.True(historyStore.AfterQueryCalled);
        Assert.Equal(100, historyStore.AfterReceivedAtMs);
        Assert.Equal("msg-1", historyStore.AfterMessageId);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal("msg-2", Assert.Single(catchUp.Items).MessageId);
        var persisted = Assert.Single(deviceStore.Upserted);
        Assert.Equal("msg-2", persisted.AfterMessageId);
    }

    private static DefaultSyncBootstrapQueryProcessor CreateProcessor(
        CapturingConversationStore conversationStore,
        CapturingHistoryStore historyStore,
        IRealtimeDeviceSyncCursorStore deviceStore,
        SyncBootstrapOptions? options = null) =>
        new(
            conversationStore,
            historyStore,
            deviceStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance),
            options);

    private sealed class CapturingConversationStore : IRealtimeConversationStore
    {
        private readonly IReadOnlyList<ConversationListItem> _items;

        public CapturingConversationStore(IReadOnlyList<ConversationListItem> items)
        {
            _items = items;
        }

        public Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
            long userId,
            bool? beforeIsPinned,
            long? beforePinnedAtMs,
            long? beforeLastMessageAtMs,
            string? beforeConversationId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult(_items);

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

    private sealed class CapturingHistoryStore : IRealtimeMessageHistoryStore
    {
        private readonly string _conversationId;
        private readonly IReadOnlyList<RealtimeHistoryMessage> _messages;

        public CapturingHistoryStore(
            string conversationId,
            IReadOnlyList<RealtimeHistoryMessage> messages)
        {
            _conversationId = conversationId;
            _messages = messages;
        }

        public bool ConversationQueryCalled { get; private set; }
        public bool AfterQueryCalled { get; private set; }
        public long AfterReceivedAtMs { get; private set; }
        public string? AfterMessageId { get; private set; }
        public HashSet<string> MemberConversationIds { get; init; } = new(StringComparer.Ordinal);
        public List<string> FilteredConversationIds { get; } = [];
        public List<string> QueriedConversationIds { get; } = [];

        public Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
            long userId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<ConversationMessageHistoryResult> QueryByConversationAsync(
            long userId,
            string conversationId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default)
        {
            ConversationQueryCalled = true;
            QueriedConversationIds.Add(conversationId);
            if (MemberConversationIds.Count > 0 && !MemberConversationIds.Contains(conversationId))
                return Task.FromResult(ConversationMessageHistoryResult.Forbidden);

            return Task.FromResult(ConversationMessageHistoryResult.Ok(
                string.Equals(conversationId, _conversationId, StringComparison.Ordinal)
                    ? _messages
                    : Array.Empty<RealtimeHistoryMessage>()));
        }

        public Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
            long userId,
            string conversationId,
            long afterReceivedAtMs,
            string afterMessageId,
            int take,
            CancellationToken ct = default)
        {
            AfterQueryCalled = true;
            AfterReceivedAtMs = afterReceivedAtMs;
            AfterMessageId = afterMessageId;
            QueriedConversationIds.Add(conversationId);
            if (MemberConversationIds.Count > 0 && !MemberConversationIds.Contains(conversationId))
                return Task.FromResult(ConversationMessageHistoryResult.Forbidden);

            if (!string.Equals(conversationId, _conversationId, StringComparison.Ordinal))
                return Task.FromResult(ConversationMessageHistoryResult.Ok(Array.Empty<RealtimeHistoryMessage>()));

            var after = _messages
                .Where(m =>
                    m.ReceivedAtMs > afterReceivedAtMs
                    || (m.ReceivedAtMs == afterReceivedAtMs
                        && string.CompareOrdinal(m.MessageId, afterMessageId) > 0))
                .Take(take)
                .ToArray();
            return Task.FromResult(ConversationMessageHistoryResult.Ok(after));
        }

        public Task<bool> IsConversationMemberAsync(
            long userId,
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult(
                MemberConversationIds.Count == 0
                || MemberConversationIds.Contains(conversationId));

        public Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
            long userId,
            IReadOnlyCollection<string> conversationIds,
            CancellationToken ct = default)
        {
            FilteredConversationIds.AddRange(conversationIds);
            var allowed = conversationIds
                .Where(id =>
                    MemberConversationIds.Count == 0
                    || MemberConversationIds.Contains(id))
                .ToHashSet(StringComparer.Ordinal);
            return Task.FromResult<IReadOnlySet<string>>(allowed);
        }

        public async Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
            long userId,
            IReadOnlyList<HistoryCatchUpQuery> queries,
            CancellationToken ct = default)
        {
            var map = new Dictionary<string, IReadOnlyList<RealtimeHistoryMessage>>(StringComparer.Ordinal);
            foreach (var query in queries)
            {
                ConversationMessageHistoryResult result;
                if (query.AfterReceivedAtMs is long afterAt
                    && !string.IsNullOrWhiteSpace(query.AfterMessageId))
                {
                    result = await QueryByConversationAfterAsync(
                            userId,
                            query.ConversationId,
                            afterAt,
                            query.AfterMessageId,
                            query.Take,
                            ct)
                        .ConfigureAwait(false);
                }
                else
                {
                    result = await QueryByConversationAsync(
                            userId,
                            query.ConversationId,
                            beforeReceivedAtMs: null,
                            beforeMessageId: null,
                            take: query.Take,
                            ct)
                        .ConfigureAwait(false);
                }

                map[query.ConversationId] = result.IsMember
                    ? result.Messages
                    : Array.Empty<RealtimeHistoryMessage>();
            }

            return map;
        }

        public Task<RealtimeHistoryMessage?> TryGetByIdAsync(
            string messageId,
            CancellationToken ct = default) =>
            Task.FromResult(_messages.FirstOrDefault(m => m.MessageId == messageId));

        public Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
            IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
            CancellationToken ct = default)
        {
            var map = new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal);
            foreach (var item in watermarks)
            {
                if (string.IsNullOrWhiteSpace(item.ConversationId)
                    || string.IsNullOrWhiteSpace(item.AfterMessageId)
                    || item.AfterReceivedAtMs <= 0)
                {
                    continue;
                }

                var tipAt = item.TipReceivedAtMs;
                var tipId = item.TipMessageId;
                if (tipAt is not > 0 || string.IsNullOrWhiteSpace(tipId))
                {
                    var tip = _messages
                        .Where(m => string.Equals(m.ConversationId, item.ConversationId, StringComparison.Ordinal))
                        .OrderByDescending(m => m.ReceivedAtMs)
                        .ThenByDescending(m => m.MessageId, StringComparer.Ordinal)
                        .FirstOrDefault();
                    if (tip is null)
                    {
                        map[item.ConversationId] = new ResolvedSyncWatermark
                        {
                            ConversationId = item.ConversationId,
                            AfterReceivedAtMs = 0,
                            AfterMessageId = string.Empty,
                            IsValid = false,
                            InvalidationKind = SyncWatermarkInvalidationKind.MessageNotFound,
                            TipReceivedAtMs = null,
                            TipMessageId = null,
                            ClientAfterReceivedAtMs = item.AfterReceivedAtMs,
                            ClientAfterMessageId = item.AfterMessageId
                        };
                        continue;
                    }

                    tipAt = tip.ReceivedAtMs;
                    tipId = tip.MessageId;
                }

                // Match on message id only (aligned with Npgsql resolve SQL).
                var matched = _messages.FirstOrDefault(m =>
                    string.Equals(m.ConversationId, item.ConversationId, StringComparison.Ordinal)
                    && string.Equals(m.MessageId, item.AfterMessageId, StringComparison.Ordinal));

                if (matched is not null
                    && (matched.ReceivedAtMs < tipAt
                        || (matched.ReceivedAtMs == tipAt
                            && string.CompareOrdinal(matched.MessageId, tipId) <= 0)))
                {
                    map[item.ConversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = item.ConversationId,
                        AfterReceivedAtMs = matched.ReceivedAtMs,
                        AfterMessageId = matched.MessageId,
                        IsValid = true,
                        TipReceivedAtMs = tipAt,
                        TipMessageId = tipId,
                        ClientAfterReceivedAtMs = item.AfterReceivedAtMs,
                        ClientAfterMessageId = item.AfterMessageId
                    };
                }
                else if (matched is not null)
                {
                    map[item.ConversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = item.ConversationId,
                        AfterReceivedAtMs = tipAt.Value,
                        AfterMessageId = tipId!,
                        IsValid = false,
                        InvalidationKind = SyncWatermarkInvalidationKind.AheadOfTip,
                        TipReceivedAtMs = tipAt,
                        TipMessageId = tipId,
                        ClientAfterReceivedAtMs = item.AfterReceivedAtMs,
                        ClientAfterMessageId = item.AfterMessageId
                    };
                }
                else
                {
                    map[item.ConversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = item.ConversationId,
                        AfterReceivedAtMs = tipAt.Value,
                        AfterMessageId = tipId!,
                        IsValid = false,
                        InvalidationKind = SyncWatermarkInvalidationKind.MessageNotFound,
                        TipReceivedAtMs = tipAt,
                        TipMessageId = tipId,
                        ClientAfterReceivedAtMs = item.AfterReceivedAtMs,
                        ClientAfterMessageId = item.AfterMessageId
                    };
                }
            }

            return Task.FromResult<IReadOnlyDictionary<string, ResolvedSyncWatermark>>(map);
        }
    }

    private sealed class NoopDeviceCursorStore : IRealtimeDeviceSyncCursorStore
    {
        public Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
            long userId,
            ulong deviceIdHash,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<DeviceSyncCursor>>([]);

        public Task UpsertManyAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<DeviceSyncCursor> cursors,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task DeleteAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<string> conversationIds,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }

    private sealed class CapturingDeviceCursorStore : IRealtimeDeviceSyncCursorStore
    {
        private readonly IReadOnlyList<DeviceSyncCursor> _stored;

        public CapturingDeviceCursorStore(IReadOnlyList<DeviceSyncCursor> stored)
        {
            _stored = stored;
        }

        public List<DeviceSyncCursor> Upserted { get; } = [];
        public List<string> Deleted { get; } = [];

        public Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
            long userId,
            ulong deviceIdHash,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult(_stored);

        public Task UpsertManyAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<DeviceSyncCursor> cursors,
            CancellationToken ct = default)
        {
            Upserted.AddRange(cursors);
            return Task.CompletedTask;
        }

        public Task DeleteAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<string> conversationIds,
            CancellationToken ct = default)
        {
            Deleted.AddRange(conversationIds);
            return Task.CompletedTask;
        }

        public Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }
}

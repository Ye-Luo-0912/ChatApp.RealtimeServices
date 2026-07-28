using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeBusinessEventContractTests
{
    [Theory]
    [InlineData(RealtimeEventNames.ConversationChanged, RealtimeEventType.ConversationListChanged)]
    [InlineData(RealtimeEventNames.UnreadCountChanged, RealtimeEventType.UnreadCountChanged)]
    [InlineData(RealtimeEventNames.MessageReceived, RealtimeEventType.MessageReceived)]
    [InlineData(RealtimeEventNames.MessageReceiptUpdated, RealtimeEventType.MessageReceiptUpdated)]
    [InlineData(RealtimeEventNames.SessionInvalidated, RealtimeEventType.SessionRevoked)]
    [InlineData(RealtimeEventNames.MessageRecalled, RealtimeEventType.MessageRecalled)]
    [InlineData(RealtimeEventNames.MessageEdited, RealtimeEventType.MessageEdited)]
    [InlineData(RealtimeEventNames.ReactionAdded, RealtimeEventType.ReactionAdded)]
    [InlineData(RealtimeEventNames.ReactionRemoved, RealtimeEventType.ReactionRemoved)]
    [InlineData(RealtimeEventNames.ConversationRead, RealtimeEventType.ConversationRead)]
    public void BusinessName_MapsToStableWireType(string businessName, RealtimeEventType expected)
    {
        Assert.Equal(expected, RealtimeEventTypeMapper.ToWireType(businessName));
    }

    [Fact]
    public void EventIds_AreDeterministicAndStableLength()
    {
        var messageId = MessageEventIdFactory.CreateMessageReceivedEventId(1001, "client-1");
        var echoId = MessageEventIdFactory.CreateSenderEchoEventId("msg-1", 1001);
        var convId = ConversationEventIdFactory.CreateConversationChangedEventId("dm:1:2", "msg-1", 1);
        var unreadId = ConversationEventIdFactory.CreateUnreadCountChangedEventId(
            "dm:1:2",
            1,
            3,
            "msg-1",
            10,
            "cause-1");
        var receiptId = MessageEventIdFactory.CreateMessageReceiptUpdatedEventId(
            "msg-1",
            2,
            MessageReceiptType.Read);

        Assert.Equal(64, messageId.Length);
        Assert.Equal(messageId, MessageEventIdFactory.CreateMessageReceivedEventId(1001, "client-1"));
        Assert.Equal(echoId, MessageEventIdFactory.CreateSenderEchoEventId("msg-1", 1001));
        Assert.Equal(convId, ConversationEventIdFactory.CreateConversationChangedEventId("dm:1:2", "msg-1", 1));
        Assert.Equal(
            unreadId,
            ConversationEventIdFactory.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                3,
                "msg-1",
                10,
                "cause-1"));
        Assert.NotEqual(
            unreadId,
            ConversationEventIdFactory.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                3,
                "msg-1",
                10,
                "cause-2"));
        Assert.NotEqual(
            ConversationEventIdFactory.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                1,
                null,
                null,
                "msg-a"),
            ConversationEventIdFactory.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                1,
                null,
                null,
                "msg-b"));
        var revokeId = SessionEventIdFactory.CreateSessionRevokedEventId(1, "sess-1", 99);
        Assert.Equal(
            revokeId,
            SessionEventIdFactory.CreateSessionRevokedEventId(1, "sess-1", 99));
        Assert.NotEqual(
            revokeId,
            SessionEventIdFactory.CreateSessionRevokedEventId(1, "sess-2", 99));
        Assert.Equal(
            receiptId,
            MessageEventIdFactory.CreateMessageReceiptUpdatedEventId("msg-1", 2, MessageReceiptType.Read));
        Assert.NotEqual(messageId, echoId);

        var conversationReadId = ConversationEventIdFactory.CreateConversationReadEventId(
            "grp:abc",
            2,
            "msg-1",
            10,
            1);
        Assert.Equal(
            conversationReadId,
            ConversationEventIdFactory.CreateConversationReadEventId("grp:abc", 2, "msg-1", 10, 1));
        Assert.NotEqual(
            conversationReadId,
            ConversationEventIdFactory.CreateConversationReadEventId("grp:abc", 2, "msg-1", 10, 3));
    }

    [Fact]
    public void ChatMessagePayload_RoundTripsWithPayloadVersion()
    {
        var payload = new RealtimeChatMessagePayload
        {
            MessageId = "m1",
            ClientMessageId = "c1",
            SenderUserId = 1,
            SenderSessionId = "s1",
            ReceiverUserId = 2,
            ConversationId = "dm:1:2",
            Content = "hi",
            ReceivedAtMs = 99
        };

        var json = System.Text.Json.JsonSerializer.Serialize(
            payload,
            ChatApp.Realtime.Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default
                .RealtimeChatMessagePayload);
        var restored = RealtimeWireSerializer.DeserializeChatMessage(json);
        Assert.NotNull(restored);
        Assert.Equal(RealtimeChatMessagePayload.CurrentPayloadVersion, restored.PayloadVersion);
        Assert.Equal(payload.ConversationId, restored.ConversationId);
    }

    [Fact]
    public void ReceiptPayload_RoundTripsDeliveredAndRead()
    {
        foreach (var type in new[] { MessageReceiptType.Delivered, MessageReceiptType.Read })
        {
            var payload = new RealtimeMessageReceiptPayload
            {
                MessageId = "m1",
                ReceiverUserId = 2,
                ReceiptType = type,
                OccurredAtMs = 100,
                ConversationId = "dm:1:2"
            };
            var json = System.Text.Json.JsonSerializer.Serialize(
                payload,
                ChatApp.Realtime.Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default
                    .RealtimeMessageReceiptPayload);
            var restored = RealtimeWireSerializer.DeserializeMessageReceipt(json);
            Assert.NotNull(restored);
            Assert.Equal(RealtimeMessageReceiptPayload.CurrentPayloadVersion, restored.PayloadVersion);
            Assert.Equal(type, restored.ReceiptType);
        }
    }

    [Fact]
    public void ConversationAndUnreadPayloads_CarryPayloadVersion()
    {
        var conversation = new RealtimeConversationChangedPayload
        {
            ConversationId = "dm:1:2",
            LastMessageId = "m1",
            LastMessageAtMs = 1,
            PeerUserId = 2
        };
        var unread = new RealtimeUnreadCountChangedPayload
        {
            ConversationId = "dm:1:2",
            UnreadCount = 1
        };

        Assert.Equal(3, conversation.PayloadVersion);
        Assert.Equal(1, unread.PayloadVersion);

        var conversationJson = System.Text.Json.JsonSerializer.Serialize(
            conversation,
            ChatApp.Realtime.Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default
                .RealtimeConversationChangedPayload);
        var unreadJson = System.Text.Json.JsonSerializer.Serialize(
            unread,
            ChatApp.Realtime.Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default
                .RealtimeUnreadCountChangedPayload);

        Assert.NotNull(RealtimeWireSerializer.DeserializeConversationChanged(conversationJson));
        Assert.NotNull(RealtimeWireSerializer.DeserializeUnreadCountChanged(unreadJson));
    }
}

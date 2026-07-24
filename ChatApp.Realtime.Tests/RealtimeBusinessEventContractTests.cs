using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeBusinessEventContractTests
{
    [Theory]
    [InlineData(RealtimeEventContracts.ConversationChanged, RealtimeEventType.ConversationListChanged)]
    [InlineData(RealtimeEventContracts.UnreadCountChanged, RealtimeEventType.UnreadCountChanged)]
    [InlineData(RealtimeEventContracts.MessageReceived, RealtimeEventType.MessageReceived)]
    [InlineData(RealtimeEventContracts.MessageReceiptUpdated, RealtimeEventType.MessageReceiptUpdated)]
    [InlineData(RealtimeEventContracts.SessionInvalidated, RealtimeEventType.SessionRevoked)]
    [InlineData(RealtimeEventContracts.MessageRecalled, RealtimeEventType.MessageRecalled)]
    [InlineData(RealtimeEventContracts.MessageEdited, RealtimeEventType.MessageEdited)]
    public void BusinessName_MapsToStableWireType(string businessName, RealtimeEventType expected)
    {
        Assert.Equal(expected, RealtimeEventContracts.ToWireType(businessName));
    }

    [Fact]
    public void EventIds_AreDeterministicAndStableLength()
    {
        var messageId = RealtimeEventContracts.CreateMessageReceivedEventId(1001, "client-1");
        var echoId = RealtimeEventContracts.CreateSenderEchoEventId("msg-1", 1001);
        var convId = RealtimeEventContracts.CreateConversationChangedEventId("dm:1:2", "msg-1", 1);
        var unreadId = RealtimeEventContracts.CreateUnreadCountChangedEventId(
            "dm:1:2",
            1,
            3,
            "msg-1",
            10,
            "cause-1");
        var receiptId = RealtimeEventContracts.CreateMessageReceiptUpdatedEventId(
            "msg-1",
            2,
            MessageReceiptType.Read);

        Assert.Equal(64, messageId.Length);
        Assert.Equal(messageId, RealtimeEventContracts.CreateMessageReceivedEventId(1001, "client-1"));
        Assert.Equal(echoId, RealtimeEventContracts.CreateSenderEchoEventId("msg-1", 1001));
        Assert.Equal(convId, RealtimeEventContracts.CreateConversationChangedEventId("dm:1:2", "msg-1", 1));
        Assert.Equal(
            unreadId,
            RealtimeEventContracts.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                3,
                "msg-1",
                10,
                "cause-1"));
        Assert.NotEqual(
            unreadId,
            RealtimeEventContracts.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                3,
                "msg-1",
                10,
                "cause-2"));
        Assert.NotEqual(
            RealtimeEventContracts.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                1,
                null,
                null,
                "msg-a"),
            RealtimeEventContracts.CreateUnreadCountChangedEventId(
                "dm:1:2",
                1,
                1,
                null,
                null,
                "msg-b"));
        var revokeId = RealtimeEventContracts.CreateSessionRevokedEventId(1, "sess-1", 99);
        Assert.Equal(
            revokeId,
            RealtimeEventContracts.CreateSessionRevokedEventId(1, "sess-1", 99));
        Assert.NotEqual(
            revokeId,
            RealtimeEventContracts.CreateSessionRevokedEventId(1, "sess-2", 99));
        Assert.Equal(
            receiptId,
            RealtimeEventContracts.CreateMessageReceiptUpdatedEventId("msg-1", 2, MessageReceiptType.Read));
        Assert.NotEqual(messageId, echoId);
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

        Assert.Equal(2, conversation.PayloadVersion);
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

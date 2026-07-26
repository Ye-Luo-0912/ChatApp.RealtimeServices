using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messages;

/// <summary>
/// 实时消息相关事件的纯工厂：负责把已绑定附件回填到 <see cref="RealtimeChatMessagePayload"/>、
/// 构造群消息聚合事件、发送方多设备回声事件，以及回执/带 MessageId 拷贝等。
/// 与 SQL/事务无关，便于在 <c>NpgsqlRealtimeMessageStore</c> 编排时集中调用。
/// </summary>
internal static class RealtimeMessageEventFactory
{
    public static RealtimeEvent EnrichChatMessagePayload(
        RealtimeEvent evt,
        IReadOnlyList<AttachmentRef>? attachments)
    {
        if (string.IsNullOrWhiteSpace(evt.PayloadJson))
            return evt;

        RealtimeChatMessagePayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize(
                evt.PayloadJson,
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
        }
        catch (JsonException)
        {
            return evt;
        }

        if (payload is null)
            return evt;

        var enriched = new RealtimeChatMessagePayload
        {
            PayloadVersion = RealtimeChatMessagePayload.CurrentPayloadVersion,
            MessageId = payload.MessageId,
            ClientMessageId = payload.ClientMessageId,
            SenderUserId = payload.SenderUserId,
            SenderSessionId = payload.SenderSessionId,
            ReceiverUserId = payload.ReceiverUserId,
            Content = payload.Content,
            ConversationId = payload.ConversationId,
            ReceivedAtMs = payload.ReceivedAtMs,
            Attachments = attachments is { Count: > 0 } ? attachments : payload.Attachments,
            ReplyToMessageId = payload.ReplyToMessageId,
            ReplyToSenderUserId = payload.ReplyToSenderUserId,
            ReplyToPreview = payload.ReplyToPreview,
            ForwardedFromMessageId = payload.ForwardedFromMessageId,
            ForwardedFromSenderUserId = payload.ForwardedFromSenderUserId,
            ForwardedFromPreview = payload.ForwardedFromPreview,
            MentionedUserIds = payload.MentionedUserIds,
            MentionedRoles = payload.MentionedRoles
        };

        return new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = JsonSerializer.Serialize(
                enriched,
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };
    }

    /// <summary>
    /// 群消息聚合事件：单个事件携带全部群成员作为 <see cref="RealtimeEvent.TargetUserIds"/>，
    /// 避免对每个成员产生独立 Outbox 行（O(N²) → O(N)）。
    /// </summary>
    public static RealtimeEvent CreateGroupMessageAggregatedEvent(
        RealtimeEvent template,
        RealtimeMessageRecord message,
        IReadOnlyList<long> targetUserIds)
    {
        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateGroupMessageReceivedEventId(
                message.SenderUserId,
                message.ClientMessageId,
                message.ConversationId!),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = message.SenderUserId,
            ActorUserId = message.SenderUserId,
            MessageId = message.MessageId,
            SessionId = message.SenderSessionId,
            PayloadJson = template.PayloadJson,
            OccurredAtMs = template.OccurredAtMs,
            TraceParent = template.TraceParent,
            TraceState = template.TraceState,
            TargetUserIds = targetUserIds.ToArray()
        };
    }

    /// <summary>
    /// 发送方其他在线设备回声事件：Gateway 会跳过来源 SessionId。
    /// </summary>
    public static RealtimeEvent CreateSenderEchoEvent(RealtimeEvent receiverEvent, long senderUserId)
    {
        var messageId = receiverEvent.MessageId ?? string.Empty;
        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateSenderEchoEventId(messageId, senderUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = senderUserId,
            ActorUserId = receiverEvent.ActorUserId,
            MessageId = receiverEvent.MessageId,
            SessionId = receiverEvent.SessionId,
            PayloadJson = receiverEvent.PayloadJson,
            OccurredAtMs = receiverEvent.OccurredAtMs,
            TraceParent = receiverEvent.TraceParent,
            TraceState = receiverEvent.TraceState
        };
    }

    public static RealtimeEvent CopyForReceipt(RealtimeEvent evt, long senderUserId) => new()
    {
        EventId = evt.EventId,
        Type = evt.Type,
        TargetUserId = senderUserId,
        ActorUserId = evt.ActorUserId,
        MessageId = evt.MessageId,
        SessionId = evt.SessionId,
        PayloadJson = evt.PayloadJson,
        OccurredAtMs = evt.OccurredAtMs,
        TraceParent = evt.TraceParent,
        TraceState = evt.TraceState
    };

    public static RealtimeEvent CopyWithMessageId(RealtimeEvent evt, string messageId) => new()
    {
        EventId = evt.EventId,
        Type = evt.Type,
        TargetUserId = evt.TargetUserId,
        ActorUserId = evt.ActorUserId,
        MessageId = messageId,
        SessionId = evt.SessionId,
        PayloadJson = evt.PayloadJson,
        OccurredAtMs = evt.OccurredAtMs,
        TraceParent = evt.TraceParent,
        TraceState = evt.TraceState
    };
}

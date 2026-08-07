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
        IReadOnlyList<AttachmentRef>? attachments,
        long? conversationSequence = null,
        ConversationType? conversationType = null,
        long[]? targetUserIds = null)
    {
        // P1-4：优先使用应用层传入的 Payload 对象，省去一次 deserialize + reserialize。
        // 仅当 Payload 缺失（如旧调用方/测试）且 PayloadJson 存在时，才回退到反序列化路径。
        RealtimeChatMessagePayload? payload = null;
        if (evt.Payload is RealtimeChatMessagePayload typedPayload)
        {
            payload = typedPayload;
        }
        else if (!string.IsNullOrWhiteSpace(evt.PayloadJson))
        {
            try
            {
                payload = JsonSerializer.Deserialize(
                    evt.PayloadJson,
                    RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
            }
            catch (JsonException)
            {
                return CopyWithTargetUserIds(evt, targetUserIds);
            }
        }

        if (payload is null)
            return CopyWithTargetUserIds(evt, targetUserIds);

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
            // 三-1：序列进入消息协议。优先用传入的序列号；缺省时回退到 payload 已有值。
            ConversationSequence = conversationSequence ?? payload.ConversationSequence,
            // 极限-1：会话类型进入消息协议，单事件即可驱动会话列表更新。
            ConversationType = conversationType ?? payload.ConversationType,
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

        // 一次性物化：把 enriched 对象序列化为 PayloadJson。
        // 此后所有派生事件（聚合 / 回声 / 拷贝）直接复用此 PayloadJson，不再重复序列化。
        var payloadJson = JsonSerializer.Serialize(
            enriched,
            RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);

        return new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = payloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState,
            TargetUserIds = targetUserIds
            // Payload 故意不传递：已物化为 PayloadJson，避免持有冗余引用。
        };
    }

    /// <summary>
    /// 群消息聚合事件：单个事件携带全部群成员作为 <see cref="RealtimeEvent.TargetUserIds"/>，
    /// 避免对每个成员产生独立 Outbox 行（O(N²) → O(N)）。
    /// <para>
    /// P0-3：<paramref name="targetUserIds"/> 为 null 时表示群广播（AudienceKind=Conversation），
    /// 由 <see cref="Projections.GroupProjectionDelta.AddBroadcast"/> 烙印 AudienceKind + ConversationId，
    /// TargetUserIds 保持 null，Publisher 通过会话级路由目录投递。
    /// </para>
    /// </summary>
    public static RealtimeEvent CreateGroupMessageAggregatedEvent(
        RealtimeEvent template,
        RealtimeMessageRecord message,
        IReadOnlyList<long>? targetUserIds)
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
            TargetUserIds = targetUserIds?.ToArray()
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
        TraceState = evt.TraceState,
        // P1-4：保留 Payload 对象引用，让后续 EnrichChatMessagePayload 能直接消费
        // 而不必走 PayloadJson 反序列化回退路径。
        Payload = evt.Payload
    };

    private static RealtimeEvent CopyWithTargetUserIds(
        RealtimeEvent evt,
        long[]? targetUserIds)
    {
        if (targetUserIds is null)
            return evt;

        return new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState,
            TargetUserIds = targetUserIds,
            AudienceKind = evt.AudienceKind,
            ConversationId = evt.ConversationId,
            ExcludeUserId = evt.ExcludeUserId,
            ProtocolVersion = evt.ProtocolVersion,
            AudienceVersion = evt.AudienceVersion,
            MinProtocolVersion = evt.MinProtocolVersion,
            Payload = evt.Payload
        };
    }
}

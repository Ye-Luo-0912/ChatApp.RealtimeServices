using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Postgres.Projections;

/// <summary>
/// Perf-9：群域聚合广播事件的模板工厂。
/// <para>
/// 每个方法返回一个 <b>不含 <see cref="RealtimeEvent.TargetUserIds"/></b> 的事件模板，
/// EventId 使用 target-independent 工厂派生（同一操作只产生一个 EventId）。
/// 调用方将模板交给 <see cref="GroupProjectionDelta.AddBroadcast"/>，由 Delta 烙印全体成员。
/// </para>
/// <para>
/// 这些工厂与 <see cref="Stores.ConversationWriteCommands"/> 中的 per-target 工厂并存：
/// 群路径使用本工厂（聚合），单聊路径继续使用 per-target 工厂。两套工厂共享同一 payload 类型，
/// 仅 EventId 派生方式不同（target-independent vs target-specific）。
/// </para>
/// </summary>
internal static class GroupProjectionEventFactory
{
    /// <summary>
    /// 群消息撤回广播事件模板。EventId 按 (messageId, conversationId) 派生，不纳入 target。
    /// </summary>
    public static RealtimeEvent CreateGroupMessageRecalledBroadcast(
        string messageId,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string senderSessionId,
        long recalledAtMs,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeMessageRecalledPayload
        {
            MessageId = messageId,
            ConversationId = conversationId,
            SenderUserId = senderUserId,
            ReceiverUserId = receiverUserId,
            RecalledAtMs = recalledAtMs
        };

        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateGroupMessageRecalledEventId(messageId, conversationId),
            Type = RealtimeEventType.MessageRecalled,
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = messageId,
            SessionId = senderSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeMessageRecalledPayload),
            OccurredAtMs = recalledAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    /// <summary>
    /// 群消息编辑广播事件模板。EventId 按 (messageId, conversationId, editVersion) 派生，不纳入 target。
    /// 必须纳入 editVersion，否则连续编辑会被 Outbox 冲突吞掉。
    /// <para>
    /// <paramref name="mentionedUserIds"/> / <paramref name="mentionedRoles"/> 为 <c>null</c> 时
    /// 表示本次编辑未修改 mentions，客户端应沿用上次已知值；非空数组表示编辑后替换的 mentions。
    /// </para>
    /// </summary>
    public static RealtimeEvent CreateGroupMessageEditedBroadcast(
        string messageId,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string senderSessionId,
        string content,
        int editVersion,
        long editedAtMs,
        IReadOnlyList<long>? mentionedUserIds,
        IReadOnlyList<string>? mentionedRoles,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeMessageEditedPayload
        {
            MessageId = messageId,
            ConversationId = conversationId,
            SenderUserId = senderUserId,
            ReceiverUserId = receiverUserId,
            Content = content,
            EditVersion = editVersion,
            EditedAtMs = editedAtMs,
            MentionedUserIds = mentionedUserIds,
            MentionedRoles = mentionedRoles
        };

        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateGroupMessageEditedEventId(messageId, conversationId, editVersion),
            Type = RealtimeEventType.MessageEdited,
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = messageId,
            SessionId = senderSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeMessageEditedPayload),
            OccurredAtMs = editedAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    /// <summary>
    /// 群反应广播事件模板。EventId 按 (messageId, conversationId, reactorUserId, emoji, occurredAtMs, added) 派生。
    /// </summary>
    public static RealtimeEvent CreateGroupReactionBroadcast(
        bool added,
        string messageId,
        string? conversationId,
        long reactorUserId,
        string reactorSessionId,
        long messageSenderUserId,
        long messageReceiverUserId,
        string emoji,
        int emojiCount,
        long occurredAtMs,
        string? traceParent,
        string? traceState)
    {
        string payloadJson;
        RealtimeEventType eventType;
        if (added)
        {
            eventType = RealtimeEventType.ReactionAdded;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionAddedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionAddedPayload);
        }
        else
        {
            eventType = RealtimeEventType.ReactionRemoved;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionRemovedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionRemovedPayload);
        }

        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateGroupReactionEventId(
                messageId,
                conversationId!,
                reactorUserId,
                emoji,
                occurredAtMs,
                added),
            Type = eventType,
            TargetUserId = reactorUserId,
            ActorUserId = reactorUserId,
            MessageId = messageId,
            SessionId = reactorSessionId,
            PayloadJson = payloadJson,
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    /// <summary>
    /// 群会话 tip 变更广播事件模板。EventId 按 (conversationId, lastMessageId, causeToken) 派生，不纳入 target。
    /// 用于群消息 / 编辑 / 撤回时推进会话 tip 的广播。
    /// </summary>
    public static RealtimeEvent CreateGroupConversationChangedBroadcast(
        string conversationId,
        string lastMessageId,
        string preview,
        long receivedAtMs,
        long senderUserId,
        string? causeToken,
        string? traceParent,
        string? traceState,
        long? lastSequence = null)
    {
        var payload = new RealtimeConversationChangedPayload
        {
            ConversationId = conversationId,
            Type = ConversationType.Group,
            PeerUserId = null,
            LastMessageId = lastMessageId,
            LastMessagePreview = preview,
            LastMessageAtMs = receivedAtMs,
            LastSenderUserId = senderUserId,
            LastSequence = lastSequence
        };

        return new RealtimeEvent
        {
            EventId = ConversationEventIdFactory.CreateConversationChangedAggregatedEventId(
                conversationId,
                lastMessageId,
                causeToken),
            Type = RealtimeEventType.ConversationListChanged,
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = lastMessageId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationChangedPayload),
            OccurredAtMs = receivedAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    /// <summary>
    /// 群已读水位广播事件模板。EventId 按 (conversationId, readerUserId, lastReadMessageId, lastReadAtMs) 派生，不纳入 target。
    /// 用于通知群内其他成员某用户已读到某水位。
    /// </summary>
    public static RealtimeEvent CreateGroupConversationReadBroadcast(
        string conversationId,
        long readerUserId,
        string lastReadMessageId,
        long lastReadAtMs,
        long occurredAtMs,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeConversationReadPayload
        {
            ConversationId = conversationId,
            ReaderUserId = readerUserId,
            LastReadMessageId = lastReadMessageId,
            LastReadAtMs = lastReadAtMs
        };

        return new RealtimeEvent
        {
            EventId = ConversationEventIdFactory.CreateConversationReadAggregatedEventId(
                conversationId,
                readerUserId,
                lastReadMessageId,
                lastReadAtMs),
            Type = RealtimeEventType.ConversationRead,
            TargetUserId = readerUserId,
            ActorUserId = readerUserId,
            MessageId = lastReadMessageId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationReadPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }
}

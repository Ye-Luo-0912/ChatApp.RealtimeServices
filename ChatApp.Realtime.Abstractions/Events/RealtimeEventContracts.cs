using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 实时业务事件契约：业务名、线协议枚举、EventId 幂等依据与 Payload 版本。
/// Gateway 只依赖 Abstractions DTO，不依赖 Realtime 数据库模型。
/// </summary>
public static class RealtimeEventContracts
{
    /// <summary>业务名 ConversationChanged → 线协议 <see cref="RealtimeEventType.ConversationListChanged"/>。</summary>
    public const string ConversationChanged = nameof(ConversationChanged);

    /// <summary>业务名 UnreadCountChanged → 线协议 <see cref="RealtimeEventType.UnreadCountChanged"/>。</summary>
    public const string UnreadCountChanged = nameof(UnreadCountChanged);

    /// <summary>业务名 MessageReceived → 线协议 <see cref="RealtimeEventType.MessageReceived"/>。</summary>
    public const string MessageReceived = nameof(MessageReceived);

    /// <summary>
    /// 业务名 MessageDelivered / MessageRead → 线协议
    /// <see cref="RealtimeEventType.MessageReceiptUpdated"/> + <see cref="MessageReceiptType"/>。
    /// </summary>
    public const string MessageReceiptUpdated = nameof(MessageReceiptUpdated);

    /// <summary>业务名 SessionInvalidated → 线协议 <see cref="RealtimeEventType.SessionRevoked"/>。</summary>
    public const string SessionInvalidated = nameof(SessionInvalidated);

    /// <summary>业务名 MessageRecalled → 线协议 <see cref="RealtimeEventType.MessageRecalled"/>。</summary>
    public const string MessageRecalled = nameof(MessageRecalled);

    /// <summary>业务名 MessageEdited → 线协议 <see cref="RealtimeEventType.MessageEdited"/>。</summary>
    public const string MessageEdited = nameof(MessageEdited);

    /// <summary>业务名 ReactionAdded → 线协议 <see cref="RealtimeEventType.ReactionAdded"/>。</summary>
    public const string ReactionAdded = nameof(ReactionAdded);

    /// <summary>业务名 ReactionRemoved → 线协议 <see cref="RealtimeEventType.ReactionRemoved"/>。</summary>
    public const string ReactionRemoved = nameof(ReactionRemoved);

    /// <summary>业务名 MemberJoined → 线协议 <see cref="RealtimeEventType.MemberJoined"/>。</summary>
    public const string MemberJoined = nameof(MemberJoined);

    /// <summary>业务名 MemberLeft → 线协议 <see cref="RealtimeEventType.MemberLeft"/>。</summary>
    public const string MemberLeft = nameof(MemberLeft);

    /// <summary>业务名 MemberRemoved → 线协议 <see cref="RealtimeEventType.MemberRemoved"/>。</summary>
    public const string MemberRemoved = nameof(MemberRemoved);

    /// <summary>业务名 RoleChanged → 线协议 <see cref="RealtimeEventType.RoleChanged"/>。</summary>
    public const string RoleChanged = nameof(RoleChanged);

    /// <summary>业务名 ConversationRead → 线协议 <see cref="RealtimeEventType.ConversationRead"/>。</summary>
    public const string ConversationRead = nameof(ConversationRead);

    public static string CreateMessageReceivedEventId(long senderUserId, string clientMessageId)
    {
        var input = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 群消息按目标用户拆分 Outbox 行时使用；纳入 target 避免冲突吞掉。
    /// </summary>
    public static string CreateMessageReceivedEventId(
        long senderUserId,
        string clientMessageId,
        long targetUserId)
    {
        var input = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}:{targetUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 群聊聚合事件的幂等键：按 (senderUserId, clientMessageId, conversationId) 派生，
    /// 不再按 target 拆分。同一群消息只产生一个 EventId，避免 per-member 拆分时的冲突吞掉。
    /// </summary>
    public static string CreateGroupMessageReceivedEventId(
        long senderUserId,
        string clientMessageId,
        string conversationId)
    {
        var input = Encoding.UTF8.GetBytes($"grpmsg:{senderUserId}:{clientMessageId}:{conversationId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateSenderEchoEventId(string messageId, long senderUserId)
    {
        var input = Encoding.UTF8.GetBytes($"msgecho:{messageId}:{senderUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateConversationChangedEventId(
        string conversationId,
        string lastMessageId,
        long targetUserId,
        string? causeToken = null)
    {
        var input = Encoding.UTF8.GetBytes(
            string.IsNullOrEmpty(causeToken)
                ? $"convchg:{conversationId}:{lastMessageId}:{targetUserId}"
                : $"convchg:{conversationId}:{lastMessageId}:{targetUserId}:{causeToken}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 未读变更事件幂等 Id。必须纳入 <paramref name="causeMessageId"/>（触发本次投影的消息），
    /// 否则「未读1 → 已读0 → 再未读1」会生成相同 EventId，被 Outbox 冲突吞掉。
    /// </summary>
    public static string CreateUnreadCountChangedEventId(
        string conversationId,
        long targetUserId,
        int unreadCount,
        string? lastReadMessageId,
        long? lastReadAtMs,
        string causeMessageId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(causeMessageId);
        var input = Encoding.UTF8.GetBytes(
            $"unread:{conversationId}:{targetUserId}:{unreadCount}:{lastReadAtMs ?? 0}:{lastReadMessageId ?? string.Empty}:{causeMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateSessionRevokedEventId(
        long targetUserId,
        string sessionId,
        long occurredAtMs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        var input = Encoding.UTF8.GetBytes(
            $"sessrev:{targetUserId}:{sessionId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMessageReceiptUpdatedEventId(
        string messageId,
        long receiverUserId,
        MessageReceiptType receiptType)
    {
        var input = Encoding.UTF8.GetBytes(
            $"{messageId}:{receiverUserId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateConversationPrefsChangedEventId(
        string conversationId,
        long targetUserId,
        bool isPinned,
        long? pinnedAtMs,
        bool isMuted,
        long? mutedUntilMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"convprefs:{conversationId}:{targetUserId}:{isPinned}:{pinnedAtMs ?? 0}:{isMuted}:{mutedUntilMs ?? 0}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMessageRecalledEventId(string messageId, long targetUserId)
    {
        var input = Encoding.UTF8.GetBytes($"msgrecall:{messageId}:{targetUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 编辑事件幂等 Id。必须纳入 <paramref name="editVersion"/>，
    /// 否则连续编辑会被 Outbox 冲突吞掉。
    /// </summary>
    public static string CreateMessageEditedEventId(
        string messageId,
        long targetUserId,
        int editVersion)
    {
        var input = Encoding.UTF8.GetBytes($"msgedit:{messageId}:{targetUserId}:{editVersion}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 反应新增事件幂等 Id。纳入 reactor + emoji + occurredAt，避免重复点赞/取消再点被吞。
    /// </summary>
    public static string CreateReactionAddedEventId(
        string messageId,
        long targetUserId,
        long reactorUserId,
        string emoji,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"reactadd:{messageId}:{targetUserId}:{reactorUserId}:{emoji}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 反应移除事件幂等 Id。纳入 reactor + emoji + occurredAt。
    /// </summary>
    public static string CreateReactionRemovedEventId(
        string messageId,
        long targetUserId,
        long reactorUserId,
        string emoji,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"reactrm:{messageId}:{targetUserId}:{reactorUserId}:{emoji}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateAttachmentBlobsPurgeEventId(
        string cleanupEventId,
        int chunkIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cleanupEventId);
        var input = Encoding.UTF8.GetBytes($"attach-purge:{cleanupEventId}:{chunkIndex}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMemberJoinedEventId(
        string conversationId,
        long userId,
        long targetUserId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"memjoin:{conversationId}:{userId}:{targetUserId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMemberLeftEventId(
        string conversationId,
        long userId,
        long targetUserId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"memleft:{conversationId}:{userId}:{targetUserId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMemberRemovedEventId(
        string conversationId,
        long userId,
        long targetUserId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"memrm:{conversationId}:{userId}:{targetUserId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateRoleChangedEventId(
        string conversationId,
        long userId,
        ConversationMemberRole newRole,
        long targetUserId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"rolechg:{conversationId}:{userId}:{(byte)newRole}:{targetUserId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 已读水位事件幂等 Id。纳入水位消息与 target，避免重复 MarkRead / 多目标冲突吞掉。
    /// </summary>
    public static string CreateConversationReadEventId(
        string conversationId,
        long readerUserId,
        string lastReadMessageId,
        long lastReadAtMs,
        long targetUserId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(lastReadMessageId);
        var input = Encoding.UTF8.GetBytes(
            $"convread:{conversationId}:{readerUserId}:{lastReadMessageId}:{lastReadAtMs}:{targetUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static RealtimeEventType ToWireType(string businessName) =>
        businessName switch
        {
            ConversationChanged => RealtimeEventType.ConversationListChanged,
            UnreadCountChanged => RealtimeEventType.UnreadCountChanged,
            MessageReceived => RealtimeEventType.MessageReceived,
            MessageReceiptUpdated => RealtimeEventType.MessageReceiptUpdated,
            SessionInvalidated => RealtimeEventType.SessionRevoked,
            MessageRecalled => RealtimeEventType.MessageRecalled,
            MessageEdited => RealtimeEventType.MessageEdited,
            ReactionAdded => RealtimeEventType.ReactionAdded,
            ReactionRemoved => RealtimeEventType.ReactionRemoved,
            MemberJoined => RealtimeEventType.MemberJoined,
            MemberLeft => RealtimeEventType.MemberLeft,
            MemberRemoved => RealtimeEventType.MemberRemoved,
            RoleChanged => RealtimeEventType.RoleChanged,
            ConversationRead => RealtimeEventType.ConversationRead,
            _ => throw new ArgumentOutOfRangeException(nameof(businessName), businessName, null)
        };
}

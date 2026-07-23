using System.Security.Cryptography;
using System.Text;
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

    public static string CreateMessageReceivedEventId(long senderUserId, string clientMessageId)
    {
        var input = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
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
        long targetUserId)
    {
        var input = Encoding.UTF8.GetBytes(
            $"convchg:{conversationId}:{lastMessageId}:{targetUserId}");
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

    public static string CreateAttachmentBlobsPurgeEventId(
        string cleanupEventId,
        int chunkIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cleanupEventId);
        var input = Encoding.UTF8.GetBytes($"attach-purge:{cleanupEventId}:{chunkIndex}");
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
            _ => throw new ArgumentOutOfRangeException(nameof(businessName), businessName, null)
        };
}

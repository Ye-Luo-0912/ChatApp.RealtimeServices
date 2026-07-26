using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 会话相关实时业务事件幂等 Id 工厂。
/// </summary>
public static class ConversationEventIdFactory
{
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

    /// <summary>
    /// 聚合事件（多目标 <see cref="RealtimeEvent.TargetUserIds"/>）的幂等键派生：
    /// 不纳入 target，避免同一业务变化为每个目标生成不同 EventId 而被拆成多行 Outbox。
    /// </summary>
    public static string CreateConversationCreatedAggregatedEventId(
        string conversationId,
        string causeToken,
        long occurredAtMs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(causeToken);
        var input = Encoding.UTF8.GetBytes(
            $"convchgagg:{conversationId}:{causeToken}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }
}

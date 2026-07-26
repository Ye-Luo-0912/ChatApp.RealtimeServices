using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 群成员相关实时业务事件幂等 Id 工厂。
/// </summary>
public static class GroupEventIdFactory
{
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

    /// <summary>
    /// 批量成员加入聚合事件的幂等键：按 (conversationId, 成员指纹, occurredAtMs) 派生，
    /// 不再按 target 拆分。同一批加人只产生一个 EventId。
    /// </summary>
    public static string CreateMembersAddedEventId(
        string conversationId,
        IReadOnlyList<long> addedUserIds,
        long occurredAtMs)
    {
        var joined = addedUserIds.Count == 0
            ? string.Empty
            : string.Join(",", addedUserIds);
        var input = Encoding.UTF8.GetBytes(
            $"memsadd:{conversationId}:{joined}:{occurredAtMs}");
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

    public static string CreateMemberLeftAggregatedEventId(
        string conversationId,
        long userId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"memleftagg:{conversationId}:{userId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMemberRemovedAggregatedEventId(
        string conversationId,
        long userId,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"memrmagg:{conversationId}:{userId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateRoleChangedAggregatedEventId(
        string conversationId,
        long userId,
        ConversationMemberRole newRole,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"rolechgagg:{conversationId}:{userId}:{(byte)newRole}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }
}

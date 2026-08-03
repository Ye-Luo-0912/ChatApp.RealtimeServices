using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 关系域实时业务事件幂等 Id 工厂。
/// <para>
/// EventId 基于 (targetUserId, actorUserId, requestId/resourceId, occurredAtMs) 派生，
/// 确保同一操作重试不会产生重复事件（Outbox ON CONFLICT DO NOTHING）。
/// </para>
/// </summary>
public static class RelationshipEventIdFactory
{
    private static string Hash(string input) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(input)));

    /// <summary>
    /// 好友请求列表变更事件（发送/接受/拒绝好友请求）。
    /// </summary>
    public static string CreateFriendRequestListChangedEventId(
        long targetUserId, long actorUserId, string requestId, long occurredAtMs)
        => Hash($"freqchg:{targetUserId}:{actorUserId}:{requestId}:{occurredAtMs}");

    /// <summary>
    /// 好友列表变更事件（添加好友/删除好友）。
    /// </summary>
    public static string CreateFriendListChangedEventId(
        long targetUserId, long actorUserId, string? resourceId, long occurredAtMs)
        => Hash($"frlistchg:{targetUserId}:{actorUserId}:{resourceId ?? ""}:{occurredAtMs}");

    /// <summary>
    /// 黑名单列表变更事件（拉黑/取消拉黑）。
    /// </summary>
    public static string CreateBlockedListChangedEventId(
        long actorUserId, long targetUserId, string action, long occurredAtMs)
        => Hash($"blklistchg:{actorUserId}:{targetUserId}:{action}:{occurredAtMs}");
}

using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 关系域持久化端口：好友请求 / 友谊 / 黑名单的 CRUD。
/// <para>
/// 黑名单操作复用既有 <c>public."T_BlockRecords"</c> 表（与 <see cref="IBlockListStore"/> 共享），
/// 好友请求与友谊使用 realtime schema 新表（Migration052）。
/// </para>
/// </summary>
public interface IRelationshipStore
{
    Task<RelationshipMutatePersistResult> SendFriendRequestAsync(
        string requestId, long actorUserId, long targetUserId, string? message,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    Task<RelationshipMutatePersistResult> AcceptFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    Task<RelationshipMutatePersistResult> DeclineFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    Task<RelationshipMutatePersistResult> RemoveFriendAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    Task<RelationshipMutatePersistResult> BlockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    Task<RelationshipMutatePersistResult> UnblockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default);

    /// <summary>
    /// List friends. When <paramref name="afterChangedAtMs" /> &gt; 0, returns only
    /// rows whose created_at_ms &gt; afterChangedAtMs (incremental watermark advance).
    /// </summary>
    Task<IReadOnlyList<RelationshipListItem>> ListFriendsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default);

    /// <summary>
    /// List pending friend requests. When <paramref name="afterChangedAtMs" /> &gt; 0,
    /// returns only rows whose created_at_ms &gt; afterChangedAtMs.
    /// </summary>
    Task<IReadOnlyList<RelationshipListItem>> ListFriendRequestsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default);

    /// <summary>
    /// List blocked users. The block-list table has no change timestamp, so
    /// <paramref name="afterChangedAtMs" /> is ignored and the caller must diff
    /// client-side. Parameter kept for interface symmetry.
    /// </summary>
    Task<IReadOnlyList<RelationshipListItem>> ListBlockedUsersAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default);

    /// <summary>
    /// 读取指定列表类型从 afterSequence 起的增量变更日志（按 change_sequence 升序）。
    /// limit 上限为 limit+1 条以便调用方判断 hasMore。
    /// </summary>
    Task<IReadOnlyList<RelationshipChangeLogEntry>> ListChangesAsync(
        long userId, RelationshipListType listType, long afterSequence, int limit, CancellationToken ct = default);

    /// <summary>
    /// 读取指定列表类型仍保留的最旧 change_sequence（retention floor）。无记录时返回 0。
    /// </summary>
    Task<long> GetRelationshipRetentionFloorAsync(long userId, RelationshipListType listType, CancellationToken ct = default);
}

/// <summary>关系变更持久化结果。</summary>
public readonly record struct RelationshipMutatePersistResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    string? ResourceId,
    long? TargetUserId)
{
    public static RelationshipMutatePersistResult Ok(
        string? resourceId = null, long? targetUserId = null) =>
        new(true, null, null, resourceId, targetUserId);

    public static RelationshipMutatePersistResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null, null);
}
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 占位实现：关系存储未配置时返回失败，列表查询返回空。
/// </summary>
public sealed class NoopRelationshipStore : IRelationshipStore
{
    public Task<RelationshipMutatePersistResult> SendFriendRequestAsync(
        string requestId, long actorUserId, long targetUserId, string? message,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<RelationshipMutatePersistResult> AcceptFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<RelationshipMutatePersistResult> DeclineFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<RelationshipMutatePersistResult> RemoveFriendAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<RelationshipMutatePersistResult> BlockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<RelationshipMutatePersistResult> UnblockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
        Task.FromResult(RelationshipMutatePersistResult.Fail(
            "relationship_store_unavailable", "关系存储未配置。"));

    public Task<IReadOnlyList<RelationshipListItem>> ListFriendsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

    public Task<IReadOnlyList<RelationshipListItem>> ListFriendRequestsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

    public Task<IReadOnlyList<RelationshipListItem>> ListBlockedUsersAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

    public Task<IReadOnlyList<RelationshipChangeLogEntry>> ListChangesAsync(
        long userId, RelationshipListType listType, long afterSequence, int limit, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RelationshipChangeLogEntry>>(Array.Empty<RelationshipChangeLogEntry>());

    public Task<long> GetRelationshipRetentionFloorAsync(
        long userId, RelationshipListType listType, CancellationToken ct = default) =>
        Task.FromResult(0L);}
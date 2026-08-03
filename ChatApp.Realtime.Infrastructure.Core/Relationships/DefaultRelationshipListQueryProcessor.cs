using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

/// <summary>
/// 关系列表查询处理器：按 <see cref="RelationshipListType"/> 分发到 store。
/// </summary>
public sealed class DefaultRelationshipListQueryProcessor : IRelationshipListQueryProcessor
{
    private readonly IRelationshipStore _store;

    public DefaultRelationshipListQueryProcessor(IRelationshipStore store)
    {
        _store = store;
    }

    public async Task<RelationshipListResult> ProcessAsync(
        RelationshipListQuery query, CancellationToken ct = default)
    {
        var size = query.PageSize is null or 0 ? 50 : query.PageSize.Value;
        IReadOnlyList<RelationshipListItem> items;
        switch (query.ListType)
        {
            case RelationshipListType.Friends:
                items = await _store.ListFriendsAsync(
                    query.ActorUserId, query.PageSize, query.Cursor, afterChangedAtMs: 0, ct).ConfigureAwait(false);
                break;
            case RelationshipListType.FriendRequests:
                items = await _store.ListFriendRequestsAsync(
                    query.ActorUserId, query.PageSize, query.Cursor, afterChangedAtMs: 0, ct).ConfigureAwait(false);
                break;
            case RelationshipListType.BlockedUsers:
                items = await _store.ListBlockedUsersAsync(
                    query.ActorUserId, query.PageSize, query.Cursor, afterChangedAtMs: 0, ct).ConfigureAwait(false);
                break;
            default:
                return RelationshipListResult.Failed(
                    query.RequestId, "unknown_list_type", "未知关系列表类型。");
        }

        var hasMore = items.Count > size;
        string? nextCursor = null;
        if (hasMore)
        {
            var offset = DecodeCursor(query.Cursor) + size;
            nextCursor = Convert.ToBase64String(BitConverter.GetBytes(offset));
        }
        return RelationshipListResult.Success(query.RequestId, items, nextCursor, hasMore);
    }

    private static int DecodeCursor(string? cursor)
    {
        if (string.IsNullOrWhiteSpace(cursor)) return 0;
        try
        {
            var bytes = Convert.FromBase64String(cursor);
            return BitConverter.ToInt32(bytes, 0);
        }
        catch
        {
            return 0;
        }
    }
}
using System.Buffers.Binary;
using System.Text;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

/// <summary>
/// Opt-in Server-authoritative relationship list reader. The default registration remains
/// fail-closed; deployments may select this processor only after snapshot rebuild canary.
/// </summary>
public sealed class ProjectedRelationshipListQueryProcessor(
    IRelationshipProjectionQueryStore store) : IRelationshipListQueryProcessor
{
    private const int DefaultPageSize = 50;
    private const int MaxPageSize = 200;

    public async Task<RelationshipListResult> ProcessAsync(
        RelationshipListQuery query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (query.ActorUserId <= 0)
        {
            return RelationshipListResult.Failed(
                query.RequestId,
                "invalid_actor",
                "用户身份无效。");
        }

        var pageSize = query.PageSize is null or 0 ? DefaultPageSize : query.PageSize.Value;
        if (pageSize is < 1 or > MaxPageSize)
        {
            return RelationshipListResult.Failed(
                query.RequestId,
                "invalid_page_size",
                "分页大小必须在 1..200。");
        }

        if (!TryMapListType(query.ListType, out var projectionListType))
        {
            return RelationshipListResult.Failed(
                query.RequestId,
                "unknown_list_type",
                "未知关系列表类型。");
        }

        if (!RelationshipProjectionListCursorCodec.TryDecode(
                query.Cursor,
                out var expectedVersion,
                out var afterResourceId))
        {
            return RelationshipListResult.Failed(
                query.RequestId,
                "invalid_cursor",
                "关系列表游标无效，请从第一页重新加载。");
        }

        var page = await store.ReadAsync(
                query.ActorUserId,
                projectionListType,
                pageSize,
                expectedVersion,
                afterResourceId,
                ct)
            .ConfigureAwait(false);
        return page.Status switch
        {
            RelationshipProjectionReadStatus.Unavailable => RelationshipListResult.Failed(
                query.RequestId,
                "relationship_read_projection_unavailable",
                "关系列表投影尚未完成快照基线，请继续使用 ChatApp.Server HTTP API。"),
            RelationshipProjectionReadStatus.VersionChanged => RelationshipListResult.Failed(
                query.RequestId,
                "relationship_projection_changed",
                "关系列表在分页期间发生变化，请从第一页重新加载。"),
            RelationshipProjectionReadStatus.Ready => RelationshipListResult.Success(
                query.RequestId,
                page.Items,
                page.HasMore
                    ? RelationshipProjectionListCursorCodec.Encode(
                        page.CurrentVersion,
                        page.NextResourceId!)
                    : null,
                page.HasMore),
            _ => throw new InvalidOperationException("Unknown relationship projection read status.")
        };
    }

    private static bool TryMapListType(
        RelationshipListType source,
        out RelationshipProjectionListType target)
    {
        target = source switch
        {
            RelationshipListType.Friends => RelationshipProjectionListType.Friends,
            RelationshipListType.FriendRequests => RelationshipProjectionListType.FriendRequests,
            RelationshipListType.BlockedUsers => RelationshipProjectionListType.BlockedUsers,
            _ => default
        };
        return Enum.IsDefined(source);
    }
}

internal static class RelationshipProjectionListCursorCodec
{
    private const int VersionBytes = sizeof(long);
    private const int MaxResourceIdUtf8Bytes = 512;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static string Encode(long version, string resourceId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(version);
        ArgumentException.ThrowIfNullOrWhiteSpace(resourceId);
        var byteCount = StrictUtf8.GetByteCount(resourceId);
        if (byteCount > MaxResourceIdUtf8Bytes)
            throw new ArgumentException("Relationship resource id is too large.", nameof(resourceId));

        Span<byte> payload = stackalloc byte[VersionBytes + byteCount];
        BinaryPrimitives.WriteInt64BigEndian(payload, version);
        StrictUtf8.GetBytes(resourceId, payload[VersionBytes..]);
        return Convert.ToBase64String(payload);
    }

    public static bool TryDecode(
        string? cursor,
        out long? version,
        out string? resourceId)
    {
        version = null;
        resourceId = null;
        if (string.IsNullOrWhiteSpace(cursor))
            return true;
        if (cursor.Length > 4 * ((VersionBytes + MaxResourceIdUtf8Bytes + 2) / 3))
            return false;

        Span<byte> payload = stackalloc byte[VersionBytes + MaxResourceIdUtf8Bytes];
        if (!Convert.TryFromBase64String(cursor, payload, out var written)
            || written <= VersionBytes)
        {
            return false;
        }

        var decodedVersion = BinaryPrimitives.ReadInt64BigEndian(payload);
        if (decodedVersion < 0)
            return false;
        try
        {
            var decodedResourceId = StrictUtf8.GetString(payload[VersionBytes..written]);
            if (string.IsNullOrWhiteSpace(decodedResourceId) || decodedResourceId.Length > 128)
                return false;
            version = decodedVersion;
            resourceId = decodedResourceId;
            return true;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }
}

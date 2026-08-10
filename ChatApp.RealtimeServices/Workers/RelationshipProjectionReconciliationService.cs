using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using System.Text.Json;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// Cold-path, bounded reconciliation of Server authority and the local Realtime projection.
/// It compares stream metadata only; no relationship item is downloaded or returned.
/// </summary>
internal sealed class RelationshipProjectionReconciliationService(
    IRelationshipProjectionSnapshotSource source,
    IRelationshipProjectionOpsQueryStore localStore)
{
    private const int MaxPageSize = 100;

    public async Task<RelationshipProjectionReconciliationPageDto> ReconcileAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize,
        CancellationToken ct)
    {
        ValidateCursor(afterOwnerUserId, afterListType, pageSize);
        try
        {
            var fetchSize = pageSize + 1;
            var serverPageTask = source.ListStreamsAsync(
                afterOwnerUserId,
                afterListType,
                fetchSize,
                ct);
            var localPageTask = localStore.ListStreamsAsync(
                afterOwnerUserId,
                afterListType,
                fetchSize,
                ct);
            await Task.WhenAll(serverPageTask, localPageTask).ConfigureAwait(false);

            var serverPage = await serverPageTask.ConfigureAwait(false);
            var localPage = await localPageTask.ConfigureAwait(false);
            if (!localPage.Available)
                return Unavailable("realtime_projection_unavailable");

            var serverByKey = BuildServerIndex(serverPage.Items);
            var localByKey = BuildLocalIndex(localPage.Items);
            var keys = serverByKey.Keys
                .Concat(localByKey.Keys)
                .Distinct()
                .Order()
                .ToArray();
            if (keys.Length == 0 && (serverPage.HasMore || localPage.HasMore))
                throw new InvalidDataException("Relationship projection source did not advance its cursor.");

            var selectedKeys = keys.Take(pageSize).ToArray();
            var results = new List<RelationshipProjectionReconciliationItemDto>(selectedKeys.Length);
            foreach (var key in selectedKeys)
            {
                serverByKey.TryGetValue(key, out var server);
                localByKey.TryGetValue(key, out var local);
                RelationshipProjectionStreamDigest? digest = null;
                if (server is not null)
                {
                    digest = await source.ReadDigestAsync(
                            key.OwnerUserId,
                            key.ListType,
                            ct)
                        .ConfigureAwait(false);
                    ValidateDigest(key, digest);
                }

                results.Add(BuildResult(key, server, digest, local));
            }

            var hasMore = keys.Length > pageSize || serverPage.HasMore || localPage.HasMore;
            var next = hasMore && selectedKeys.Length > 0 ? selectedKeys[^1] : (StreamKey?)null;
            var matchedCount = results.Count(static item => item.Matches);
            return new RelationshipProjectionReconciliationPageDto(
                Available: true,
                Error: null,
                Items: results,
                MatchedCount: matchedCount,
                MismatchCount: results.Count - matchedCount,
                HasMore: hasMore,
                NextOwnerUserId: next?.OwnerUserId,
                NextListType: next?.ListType,
                GeneratedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
        catch (RelationshipProjectionSourceUnavailableException)
        {
            return Unavailable("server_projection_source_unavailable");
        }
        catch (RelationshipProjectionSourceException)
        {
            return Unavailable("server_projection_source_failed");
        }
        catch (JsonException)
        {
            return Unavailable("server_projection_source_invalid");
        }
        catch (InvalidDataException)
        {
            return Unavailable("relationship_projection_reconciliation_input_invalid");
        }
    }

    private static RelationshipProjectionReconciliationItemDto BuildResult(
        StreamKey key,
        RelationshipProjectionStreamDescriptor? server,
        RelationshipProjectionStreamDigest? digest,
        RelationshipProjectionOpsStreamDto? local)
    {
        var issues = new List<string>(8);
        if (server is null)
            issues.Add("server_stream_missing");
        if (local is null)
            issues.Add("realtime_stream_missing");
        if (server is not null && digest is not null && server.Version != digest.Version)
            issues.Add("server_version_changed_during_reconcile");

        if (digest is not null && local is not null)
        {
            if (local.CurrentVersion != digest.Version)
                issues.Add("current_version_mismatch");
            if (local.CurrentItemCount != digest.ItemCount)
                issues.Add("current_item_count_mismatch");
            if (!local.HasSnapshotBaseline)
                issues.Add("snapshot_baseline_missing");
            if (local.SnapshotVersion != digest.Version)
                issues.Add("snapshot_version_mismatch");
            if (local.SnapshotItemCount != digest.ItemCount)
                issues.Add("snapshot_item_count_mismatch");
            if (!string.Equals(
                    local.SnapshotResourceHash,
                    digest.ResourceHash,
                    StringComparison.Ordinal))
            {
                issues.Add("snapshot_resource_hash_mismatch");
            }
            if (!local.IsLocallyContiguous)
                issues.Add("local_projection_gap");
        }

        return new RelationshipProjectionReconciliationItemDto(
            OwnerUserId: key.OwnerUserId,
            ListType: key.ListType,
            Matches: issues.Count == 0,
            Issues: issues,
            ServerListedVersion: server?.Version,
            ServerDigestVersion: digest?.Version,
            ServerItemCount: digest?.ItemCount,
            ServerResourceHash: digest?.ResourceHash,
            RealtimeCurrentVersion: local?.CurrentVersion,
            RealtimeCurrentItemCount: local?.CurrentItemCount,
            RealtimeSnapshotVersion: local?.SnapshotVersion,
            RealtimeSnapshotItemCount: local?.SnapshotItemCount,
            RealtimeSnapshotResourceHash: local?.SnapshotResourceHash,
            RealtimeLocallyContiguous: local?.IsLocallyContiguous);
    }

    private static Dictionary<StreamKey, RelationshipProjectionStreamDescriptor> BuildServerIndex(
        IReadOnlyList<RelationshipProjectionStreamDescriptor> items)
    {
        var result = new Dictionary<StreamKey, RelationshipProjectionStreamDescriptor>(items.Count);
        foreach (var item in items)
        {
            var key = CreateKey(item.OwnerUserId, item.ListType);
            if (item.Version < 0)
                throw new InvalidDataException("Server relationship projection returned a negative version.");
            if (!result.TryAdd(key, item))
                throw new InvalidDataException("Server relationship projection returned a duplicate stream.");
        }

        return result;
    }

    private static Dictionary<StreamKey, RelationshipProjectionOpsStreamDto> BuildLocalIndex(
        IReadOnlyList<RelationshipProjectionOpsStreamDto> items)
    {
        var result = new Dictionary<StreamKey, RelationshipProjectionOpsStreamDto>(items.Count);
        foreach (var item in items)
        {
            var key = CreateKey(item.OwnerUserId, item.ListType);
            if (item.CurrentVersion < 0
                || item.CurrentItemCount < 0
                || item.SnapshotVersion is < 0
                || item.SnapshotItemCount is < 0
                || item.DeltaInboxCountAfterSnapshot < 0
                || item.DeltaInboxMaxVersion is < 0)
            {
                throw new InvalidDataException(
                    "Realtime relationship projection returned negative stream metadata.");
            }
            if (!result.TryAdd(key, item))
                throw new InvalidDataException("Realtime relationship projection returned a duplicate stream.");
        }

        return result;
    }

    private static void ValidateDigest(
        StreamKey expectedKey,
        RelationshipProjectionStreamDigest digest)
    {
        if (digest.OwnerUserId != expectedKey.OwnerUserId
            || digest.ListType != expectedKey.ListType
            || digest.Version < 0
            || digest.ItemCount < 0
            || string.IsNullOrEmpty(digest.ResourceHash)
            || digest.ResourceHash.Length != 64
            || digest.ResourceHash.Any(static character => !Uri.IsHexDigit(character)))
        {
            throw new InvalidDataException(
                "Server relationship projection returned an invalid stream digest.");
        }
    }

    private static StreamKey CreateKey(
        long ownerUserId,
        RelationshipProjectionListType listType)
    {
        if (ownerUserId <= 0 || !Enum.IsDefined(listType))
            throw new InvalidDataException("Relationship projection returned an invalid stream key.");
        return new StreamKey(ownerUserId, listType);
    }

    private static void ValidateCursor(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize)
    {
        if (pageSize is < 1 or > MaxPageSize)
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (afterOwnerUserId is <= 0)
            throw new ArgumentOutOfRangeException(nameof(afterOwnerUserId));
        if (afterOwnerUserId.HasValue != afterListType.HasValue)
            throw new ArgumentException("Both relationship projection cursor fields are required.");
        if (afterListType is { } listType && !Enum.IsDefined(listType))
            throw new ArgumentOutOfRangeException(nameof(afterListType));
    }

    private static RelationshipProjectionReconciliationPageDto Unavailable(string error) => new(
        Available: false,
        Error: error,
        Items: [],
        MatchedCount: 0,
        MismatchCount: 0,
        HasMore: false,
        NextOwnerUserId: null,
        NextListType: null,
        GeneratedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    private readonly record struct StreamKey(
        long OwnerUserId,
        RelationshipProjectionListType ListType) : IComparable<StreamKey>
    {
        public int CompareTo(StreamKey other)
        {
            var ownerComparison = OwnerUserId.CompareTo(other.OwnerUserId);
            return ownerComparison != 0
                ? ownerComparison
                : ((byte)ListType).CompareTo((byte)other.ListType);
        }
    }
}

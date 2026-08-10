using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.RealtimeServices.Workers;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class RelationshipProjectionReconciliationServiceTests
{
    [Fact]
    public async Task Reconcile_MergesBothSidesByKey_AndUsesStableContinuation()
    {
        var emptyHash = RelationshipProjectionSnapshotHash.Compute([]);
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamDescriptor(
                2,
                RelationshipProjectionListType.BlockedUsers,
                4),
            new RelationshipProjectionStreamDescriptor(
                3,
                RelationshipProjectionListType.FriendRequests,
                1)
        ],
        [
            new RelationshipProjectionStreamDigest(
                2,
                RelationshipProjectionListType.BlockedUsers,
                4,
                0,
                emptyHash,
                1),
            new RelationshipProjectionStreamDigest(
                3,
                RelationshipProjectionListType.FriendRequests,
                1,
                0,
                emptyHash,
                1)
        ]);
        var local = new RecordingOpsStore(
        [
            CreateLocal(1, RelationshipProjectionListType.Friends, 1, emptyHash),
            CreateLocal(2, RelationshipProjectionListType.BlockedUsers, 4, emptyHash)
        ]);
        var service = new RelationshipProjectionReconciliationService(source, local);

        var first = await service.ReconcileAsync(null, null, 2, CancellationToken.None);

        Assert.True(first.Available);
        Assert.True(first.HasMore);
        Assert.Equal(2, first.NextOwnerUserId);
        Assert.Equal(RelationshipProjectionListType.BlockedUsers, first.NextListType);
        Assert.Equal(1, first.MatchedCount);
        Assert.Equal(1, first.MismatchCount);
        Assert.Equal("server_stream_missing", Assert.Single(first.Items[0].Issues));
        Assert.True(first.Items[1].Matches);
        Assert.Equal([(2L, RelationshipProjectionListType.BlockedUsers)], source.DigestReads);

        var second = await service.ReconcileAsync(
            first.NextOwnerUserId,
            first.NextListType,
            2,
            CancellationToken.None);

        var serverOnly = Assert.Single(second.Items);
        Assert.False(serverOnly.Matches);
        Assert.Equal("realtime_stream_missing", Assert.Single(serverOnly.Issues));
        Assert.False(second.HasMore);
        Assert.Equal(0, second.MatchedCount);
        Assert.Equal(1, second.MismatchCount);
    }

    [Fact]
    public async Task Reconcile_ReportsEveryMetadataMismatchWithoutRelationshipItems()
    {
        var serverHash = RelationshipProjectionSnapshotHash.Compute(["11"]);
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamDescriptor(
                7,
                RelationshipProjectionListType.Friends,
                4)
        ],
        [
            new RelationshipProjectionStreamDigest(
                7,
                RelationshipProjectionListType.Friends,
                5,
                1,
                serverHash,
                1)
        ]);
        var local = new RecordingOpsStore(
        [
            new RelationshipProjectionOpsStreamDto(
                OwnerUserId: 7,
                ListType: RelationshipProjectionListType.Friends,
                CurrentVersion: 4,
                CurrentItemCount: 2,
                SnapshotVersion: 3,
                SnapshotItemCount: 2,
                SnapshotResourceHash: RelationshipProjectionSnapshotHash.Compute(["12"]),
                DeltaInboxCountAfterSnapshot: 0,
                DeltaInboxMaxVersion: null,
                HasSnapshotBaseline: true,
                IsLocallyContiguous: false,
                UpdatedAtMs: 1)
        ]);
        var service = new RelationshipProjectionReconciliationService(source, local);

        var report = await service.ReconcileAsync(null, null, 10, CancellationToken.None);

        var item = Assert.Single(report.Items);
        Assert.False(item.Matches);
        Assert.Contains("server_version_changed_during_reconcile", item.Issues);
        Assert.Contains("current_version_mismatch", item.Issues);
        Assert.Contains("current_item_count_mismatch", item.Issues);
        Assert.Contains("snapshot_version_mismatch", item.Issues);
        Assert.Contains("snapshot_item_count_mismatch", item.Issues);
        Assert.Contains("snapshot_resource_hash_mismatch", item.Issues);
        Assert.Contains("local_projection_gap", item.Issues);
        Assert.DoesNotContain(item.Issues, issue => issue.Contains("11", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Reconcile_FailsClosedWhenSnapshotSourceIsDisabled()
    {
        var service = new RelationshipProjectionReconciliationService(
            UnavailableRelationshipProjectionSnapshotSource.Instance,
            new RecordingOpsStore([]));

        var report = await service.ReconcileAsync(null, null, 10, CancellationToken.None);

        Assert.False(report.Available);
        Assert.Equal("server_projection_source_unavailable", report.Error);
        Assert.Empty(report.Items);
    }

    [Fact]
    public async Task Reconcile_FailsClosedWhenServerDigestIsInvalid()
    {
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamDescriptor(
                7,
                RelationshipProjectionListType.Friends,
                1)
        ],
        [
            new RelationshipProjectionStreamDigest(
                7,
                RelationshipProjectionListType.Friends,
                1,
                0,
                new string('0', 63),
                1)
        ]);
        var service = new RelationshipProjectionReconciliationService(
            source,
            new RecordingOpsStore([]));

        var report = await service.ReconcileAsync(null, null, 10, CancellationToken.None);

        Assert.False(report.Available);
        Assert.Equal("relationship_projection_reconciliation_input_invalid", report.Error);
        Assert.Empty(report.Items);
    }

    private static RelationshipProjectionOpsStreamDto CreateLocal(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long version,
        string hash) => new(
        OwnerUserId: ownerUserId,
        ListType: listType,
        CurrentVersion: version,
        CurrentItemCount: 0,
        SnapshotVersion: version,
        SnapshotItemCount: 0,
        SnapshotResourceHash: hash,
        DeltaInboxCountAfterSnapshot: 0,
        DeltaInboxMaxVersion: null,
        HasSnapshotBaseline: true,
        IsLocallyContiguous: true,
        UpdatedAtMs: 1);

    private sealed class RecordingSource(
        IReadOnlyList<RelationshipProjectionStreamDescriptor> streams,
        IReadOnlyList<RelationshipProjectionStreamDigest> digests)
        : IRelationshipProjectionSnapshotSource
    {
        private readonly Dictionary<(long, RelationshipProjectionListType), RelationshipProjectionStreamDigest>
            _digests = digests.ToDictionary(static item => (item.OwnerUserId, item.ListType));

        public List<(long, RelationshipProjectionListType)> DigestReads { get; } = [];

        public Task<RelationshipProjectionStreamPage> ListStreamsAsync(
            long? afterOwnerUserId,
            RelationshipProjectionListType? afterListType,
            int limit,
            CancellationToken ct)
        {
            var filtered = streams
                .Where(item => IsAfter(item.OwnerUserId, item.ListType, afterOwnerUserId, afterListType))
                .OrderBy(static item => item.OwnerUserId)
                .ThenBy(static item => item.ListType)
                .ToArray();
            var items = filtered.Take(limit).ToArray();
            var last = items.LastOrDefault();
            return Task.FromResult(new RelationshipProjectionStreamPage(
                items,
                filtered.Length > limit,
                filtered.Length > limit ? last?.OwnerUserId : null,
                filtered.Length > limit ? last?.ListType : null));
        }

        public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct) => throw new NotSupportedException();

        public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct)
        {
            DigestReads.Add((ownerUserId, listType));
            return Task.FromResult(_digests[(ownerUserId, listType)]);
        }
    }

    private sealed class RecordingOpsStore(
        IReadOnlyList<RelationshipProjectionOpsStreamDto> streams)
        : IRelationshipProjectionOpsQueryStore
    {
        public Task<RelationshipProjectionOpsStatusDto> GetStatusAsync(
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<RelationshipProjectionOpsStreamPageDto> ListStreamsAsync(
            long? afterOwnerUserId,
            RelationshipProjectionListType? afterListType,
            int pageSize,
            CancellationToken ct = default)
        {
            var filtered = streams
                .Where(item => IsAfter(item.OwnerUserId, item.ListType, afterOwnerUserId, afterListType))
                .OrderBy(static item => item.OwnerUserId)
                .ThenBy(static item => item.ListType)
                .ToArray();
            var items = filtered.Take(pageSize).ToArray();
            var last = items.LastOrDefault();
            return Task.FromResult(new RelationshipProjectionOpsStreamPageDto(
                Available: true,
                Items: items,
                HasMore: filtered.Length > pageSize,
                NextOwnerUserId: filtered.Length > pageSize ? last?.OwnerUserId : null,
                NextListType: filtered.Length > pageSize ? last?.ListType : null,
                GeneratedAtMs: 1));
        }
    }

    private static bool IsAfter(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType) =>
        afterOwnerUserId is null
        || ownerUserId > afterOwnerUserId
        || ownerUserId == afterOwnerUserId && listType > afterListType;
}

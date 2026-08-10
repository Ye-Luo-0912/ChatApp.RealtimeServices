using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class UnavailableRelationshipProjectionOpsQueryStore
    : IRelationshipProjectionOpsQueryStore
{
    public static UnavailableRelationshipProjectionOpsQueryStore Instance { get; } = new();

    private UnavailableRelationshipProjectionOpsQueryStore()
    {
    }

    public Task<RelationshipProjectionOpsStatusDto> GetStatusAsync(
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return Task.FromResult(new RelationshipProjectionOpsStatusDto(
            Available: false,
            PassNumber: 0,
            StablePasses: 0,
            PassChanged: false,
            CursorOwnerUserId: null,
            CursorListType: null,
            LeaseActive: false,
            LockedUntilMs: null,
            NextAttemptAtMs: 0,
            LastError: null,
            VersionStreamCount: 0,
            SnapshotBaselineStreamCount: 0,
            StreamsWithoutSnapshotBaselineCount: 0,
            ProjectionItemCount: 0,
            InboxEventCount: 0,
            UpdatedAtMs: 0,
            GeneratedAtMs: now));
    }

    public Task<RelationshipProjectionOpsStreamPageDto> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(new RelationshipProjectionOpsStreamPageDto(
            Available: false,
            Items: [],
            HasMore: false,
            NextOwnerUserId: null,
            NextListType: null,
            GeneratedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
    }
}

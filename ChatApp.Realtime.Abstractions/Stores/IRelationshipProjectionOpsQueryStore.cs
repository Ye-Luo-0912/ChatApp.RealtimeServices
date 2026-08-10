using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Read-only operational view of the Server-authoritative relationship projection.
/// It exposes stream metadata only and never returns relationship resource ids or messages.
/// </summary>
public interface IRelationshipProjectionOpsQueryStore
{
    Task<RelationshipProjectionOpsStatusDto> GetStatusAsync(
        CancellationToken ct = default);

    Task<RelationshipProjectionOpsStreamPageDto> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize,
        CancellationToken ct = default);
}

public sealed record RelationshipProjectionOpsStatusDto(
    bool Available,
    long PassNumber,
    int StablePasses,
    bool PassChanged,
    long? CursorOwnerUserId,
    RelationshipProjectionListType? CursorListType,
    bool LeaseActive,
    long? LockedUntilMs,
    long NextAttemptAtMs,
    string? LastError,
    long VersionStreamCount,
    long SnapshotBaselineStreamCount,
    long StreamsWithoutSnapshotBaselineCount,
    long ProjectionItemCount,
    long InboxEventCount,
    long UpdatedAtMs,
    long GeneratedAtMs);

public sealed record RelationshipProjectionOpsStreamDto(
    long OwnerUserId,
    RelationshipProjectionListType ListType,
    long CurrentVersion,
    long CurrentItemCount,
    long? SnapshotVersion,
    int? SnapshotItemCount,
    string? SnapshotResourceHash,
    long DeltaInboxCountAfterSnapshot,
    long? DeltaInboxMaxVersion,
    bool HasSnapshotBaseline,
    bool IsLocallyContiguous,
    long UpdatedAtMs);

public sealed record RelationshipProjectionOpsStreamPageDto(
    bool Available,
    IReadOnlyList<RelationshipProjectionOpsStreamDto> Items,
    bool HasMore,
    long? NextOwnerUserId,
    RelationshipProjectionListType? NextListType,
    long GeneratedAtMs);

/// <summary>
/// One privacy-minimized comparison between Server authority and the Realtime projection.
/// Hashes are operational digests; relationship resource ids and payloads are never returned.
/// </summary>
public sealed record RelationshipProjectionReconciliationItemDto(
    long OwnerUserId,
    RelationshipProjectionListType ListType,
    bool Matches,
    IReadOnlyList<string> Issues,
    long? ServerListedVersion,
    long? ServerDigestVersion,
    int? ServerItemCount,
    string? ServerResourceHash,
    long? RealtimeCurrentVersion,
    long? RealtimeCurrentItemCount,
    long? RealtimeSnapshotVersion,
    int? RealtimeSnapshotItemCount,
    string? RealtimeSnapshotResourceHash,
    bool? RealtimeLocallyContiguous);

public sealed record RelationshipProjectionReconciliationPageDto(
    bool Available,
    string? Error,
    IReadOnlyList<RelationshipProjectionReconciliationItemDto> Items,
    int MatchedCount,
    int MismatchCount,
    bool HasMore,
    long? NextOwnerUserId,
    RelationshipProjectionListType? NextListType,
    long GeneratedAtMs);

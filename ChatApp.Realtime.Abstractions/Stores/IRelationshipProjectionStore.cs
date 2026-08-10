using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Stores;

public enum RelationshipProjectionApplyResult : byte
{
    Applied = 1,
    Duplicate = 2,
}

public enum RelationshipProjectionSnapshotApplyResult : byte
{
    Applied = 1,
    Duplicate = 2,
}

/// <summary>
/// Applies one contiguous Server-authoritative relationship delta. Implementations must
/// commit the item projection, stream version and event inbox in one transaction.
/// </summary>
public interface IRelationshipProjectionStore
{
    Task<RelationshipProjectionApplyResult> ApplyAsync(
        RelationshipProjectionDelta delta,
        CancellationToken ct = default);

    Task<RelationshipProjectionSnapshotApplyResult> ApplySnapshotAsync(
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct = default);
}

public sealed class RelationshipProjectionGapException : InvalidOperationException
{
    public RelationshipProjectionGapException(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long expectedVersion,
        long actualVersion)
        : base(
            $"Relationship projection version gap for owner {ownerUserId}, " +
            $"list {listType}: expected {expectedVersion}, received {actualVersion}.")
    {
        OwnerUserId = ownerUserId;
        ListType = listType;
        ExpectedVersion = expectedVersion;
        ActualVersion = actualVersion;
    }

    public long OwnerUserId { get; }
    public RelationshipProjectionListType ListType { get; }
    public long ExpectedVersion { get; }
    public long ActualVersion { get; }
}

public sealed class RelationshipProjectionSnapshotVersionMismatchException : InvalidOperationException
{
    public RelationshipProjectionSnapshotVersionMismatchException(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long snapshotVersion,
        long currentVersion)
        : base(
            $"Relationship snapshot version mismatch for owner {ownerUserId}, list {listType}: " +
            $"snapshot {snapshotVersion}, projection {currentVersion}.")
    {
        OwnerUserId = ownerUserId;
        ListType = listType;
        SnapshotVersion = snapshotVersion;
        CurrentVersion = currentVersion;
    }

    public long OwnerUserId { get; }
    public RelationshipProjectionListType ListType { get; }
    public long SnapshotVersion { get; }
    public long CurrentVersion { get; }
}

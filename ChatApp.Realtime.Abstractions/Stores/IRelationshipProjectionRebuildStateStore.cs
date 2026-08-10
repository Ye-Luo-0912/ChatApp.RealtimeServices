using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Stores;

public sealed record RelationshipProjectionRebuildLease(
    string LeaseOwner,
    string ClaimToken,
    long? AfterOwnerUserId,
    RelationshipProjectionListType? AfterListType,
    long PassNumber,
    bool PassChanged,
    int StablePasses);

public sealed record RelationshipProjectionRebuildPassResult(
    long PassNumber,
    int StablePasses,
    long NextAttemptAtMs);

/// <summary>
/// Durable singleton state for the Server-authoritative relationship snapshot scan.
/// Every mutation is fenced by owner + claim token so an expired worker cannot advance
/// a lease acquired by another instance.
/// </summary>
public interface IRelationshipProjectionRebuildStateStore
{
    Task<RelationshipProjectionRebuildLease?> TryAcquireAsync(
        string leaseOwner,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task<bool> RenewAsync(
        RelationshipProjectionRebuildLease lease,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task<bool> CommitPageAsync(
        RelationshipProjectionRebuildLease lease,
        long nextOwnerUserId,
        RelationshipProjectionListType nextListType,
        bool pageChanged,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task<RelationshipProjectionRebuildPassResult?> CompletePassAsync(
        RelationshipProjectionRebuildLease lease,
        bool finalPageChanged,
        TimeSpan stablePollInterval,
        CancellationToken ct = default);

    Task<bool> ReleaseFailureAsync(
        RelationshipProjectionRebuildLease lease,
        string errorCode,
        TimeSpan retryDelay,
        CancellationToken ct = default);
}

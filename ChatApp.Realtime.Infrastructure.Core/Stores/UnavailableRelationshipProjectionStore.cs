using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// Fail-closed fallback. A versioned relationship event must not be published when its
/// durable projection store is unavailable, otherwise clients could observe a version
/// that the recovery/read model has not committed.
/// </summary>
public sealed class UnavailableRelationshipProjectionStore : IRelationshipProjectionStore
{
    public Task<RelationshipProjectionApplyResult> ApplyAsync(
        RelationshipProjectionDelta delta,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(delta);
        ct.ThrowIfCancellationRequested();
        return Task.FromException<RelationshipProjectionApplyResult>(
            new InvalidOperationException("Relationship projection store is unavailable."));
    }

    public Task<RelationshipProjectionSnapshotApplyResult> ApplySnapshotAsync(
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ct.ThrowIfCancellationRequested();
        return Task.FromException<RelationshipProjectionSnapshotApplyResult>(
            new InvalidOperationException("Relationship projection store is unavailable."));
    }
}

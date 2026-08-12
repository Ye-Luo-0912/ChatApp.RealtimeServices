using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// No-op account cleanup job store used when PostgreSQL is not configured.
/// Cleanup jobs are not acknowledged as completed without a durable store.
/// </summary>
public sealed class NoopAccountCleanupJobStore : IAccountCleanupJobStore
{
    public Task<AccountCleanupJob> EnqueueJobAsync(
        long userId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(new AccountCleanupJob(
            userId,
            AccountCleanupJob.PhaseAttachments,
            Cursor: null,
            AccountCleanupJob.StatusPending,
            RetryCount: 0,
            UpdatedAtMs: occurredAtMs));
    }

    public Task<AccountCleanupJob?> TryClaimAsync(
        long userId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<AccountCleanupJob?>(null);
    }

    public Task UpdateProgressAsync(
        long userId,
        string phase,
        string? cursor,
        string status,
        string claimToken,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task<bool> CompletePhaseAsync(
        long userId,
        string phase,
        string claimToken,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }

    public Task<bool> ReleaseToPendingAsync(
        long userId,
        string phase,
        string claimToken,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }

    public Task<AccountCleanupJob?> GetNextPendingAsync(
        string instanceId,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<AccountCleanupJob?>(null);
    }

    public Task<bool> RenewLeaseAsync(
        long userId,
        string phase,
        string claimToken,
        TimeSpan leaseExtension,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }

    public Task<bool> ProcessAttachmentsBatchAtomicAsync(
        long userId,
        string claimToken,
        string lastAttachmentId,
        IReadOnlyList<string> attachmentIds,
        RealtimeEvent purgeEvent,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }

    public Task RecordFailureAsync(
        long userId,
        string phase,
        string claimToken,
        int maxRetryCount,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }
}

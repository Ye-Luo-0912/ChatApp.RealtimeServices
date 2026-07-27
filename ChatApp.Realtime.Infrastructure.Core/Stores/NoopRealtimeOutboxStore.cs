using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeOutboxStore : IRealtimeOutboxStore
{
    public Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
        string instanceId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<RealtimeOutboxRecord>>([]);
    }

    public Task MarkPublishedAsync(RealtimeOutboxRecord record, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan retryDelay,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task MarkDeadAsync(
        RealtimeOutboxRecord record,
        string error,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task MarkPublishedBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task MarkFailedBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error, TimeSpan RetryDelay)> failures,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(failures);
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task MarkDeadBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error)> deadLetters,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(deadLetters);
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task<bool> ReplayDeadAsync(string eventId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }

    public Task<IReadOnlyList<string>> ReplayDeadBatchAsync(
        IReadOnlyList<string> eventIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventIds);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<int> CleanupPublishedAsync(
        long publishedBeforeMs,
        int batchSize,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(0);
    }

    public Task<IReadOnlyList<DeadOutboxRow>> ListDeadAsync(
        long createdBeforeMs,
        int limit,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<DeadOutboxRow>>([]);
    }

    public Task<int> DeleteDeadBatchAsync(
        IReadOnlyList<string> eventIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventIds);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(0);
    }

    public Task<RealtimeOutboxStats> GetStatsAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(new RealtimeOutboxStats(0, null, 0));
    }

    public Task<IReadOnlyList<RealtimeOutboxListItem>> ListAsync(
        RealtimeOutboxStatus? status,
        long? targetUserId,
        int offset,
        int limit,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<RealtimeOutboxListItem>>([]);
    }

    public Task<RealtimeOutboxListItem?> TryGetAsync(string eventId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<RealtimeOutboxListItem?>(null);
    }
}

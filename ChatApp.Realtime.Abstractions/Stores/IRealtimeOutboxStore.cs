namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeOutboxStore
{
    Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
        string instanceId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task MarkPublishedAsync(RealtimeOutboxRecord record, CancellationToken ct = default);

    Task MarkFailedAsync(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan retryDelay,
        CancellationToken ct = default);

    Task<RealtimeOutboxStats> GetStatsAsync(CancellationToken ct = default);
}

public sealed record RealtimeOutboxStats(
    long PendingCount,
    long? OldestPendingAtMs,
    int MaxAttemptCount);

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Age-based message hard-delete GC. Multi-instance safe via Postgres advisory lock.
/// </summary>
public interface IRealtimeMessageRetentionStore
{
    /// <summary>
    /// Deletes up to <paramref name="batchSize"/> messages with
    /// <c>received_at_ms &lt; cutoffReceivedAtMs</c>, cascading reactions and mutation-ledger
    /// orphans, then repairing conversation tips. Leaves Bound attachment rows (orphan GC
    /// elsewhere). Returns <see cref="MessageRetentionPurgeBatchResult.LockAcquired"/>=false
    /// when another instance holds the GC lease.
    /// </summary>
    Task<MessageRetentionPurgeBatchResult> TryPurgeBatchAsync(
        long cutoffReceivedAtMs,
        int batchSize,
        CancellationToken ct = default);

    /// <summary>Cheap-ish purge backlog snapshot for ops (count + oldest purgeable age).</summary>
    Task<MessageRetentionPurgeableStats> GetPurgeableStatsAsync(
        long cutoffReceivedAtMs,
        CancellationToken ct = default);
}

public sealed record MessageRetentionPurgeBatchResult(
    bool LockAcquired,
    int DeletedCount,
    int ConversationsTipRepaired);

public sealed record MessageRetentionPurgeableStats(
    long PurgeableCount,
    long? OldestReceivedAtMs);

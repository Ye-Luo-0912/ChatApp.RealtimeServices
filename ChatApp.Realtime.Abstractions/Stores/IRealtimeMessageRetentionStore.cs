namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Age-based message hard-delete GC. Multi-instance safe via Postgres advisory lock.
/// </summary>
public interface IRealtimeMessageRetentionStore
{
    /// <summary>
    /// Deletes up to <paramref name="batchSize"/> messages with
    /// <c>received_at_ms &lt; cutoffReceivedAtMs</c>, cascading reactions and mutation-ledger
    /// orphans, unbinding Bound attachments (Abandoned + <c>AttachmentBlobsPurge</c> outbox),
    /// repairing conversation tips, and recounting member <c>unread_count</c>.
    /// Returns <see cref="MessageRetentionPurgeBatchResult.LockAcquired"/>=false
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
    int ConversationsTipRepaired,
    int AttachmentsAbandoned = 0,
    int AttachmentPurgeEventsEnqueued = 0,
    int MembersUnreadRepaired = 0);

public sealed record MessageRetentionPurgeableStats(
    long PurgeableCount,
    long? OldestReceivedAtMs);

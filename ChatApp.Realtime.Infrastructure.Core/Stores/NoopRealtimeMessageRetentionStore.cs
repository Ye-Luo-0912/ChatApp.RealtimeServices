using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeMessageRetentionStore(ILogger<NoopRealtimeMessageRetentionStore> logger)
    : IRealtimeMessageRetentionStore
{
    public Task<MessageRetentionPurgeBatchResult> TryPurgeBatchAsync(
        long cutoffReceivedAtMs,
        int batchSize,
        CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop message retention purge skipped. Cutoff={Cutoff}; BatchSize={BatchSize}",
            cutoffReceivedAtMs,
            batchSize);
        return Task.FromResult(new MessageRetentionPurgeBatchResult(
            LockAcquired: true,
            DeletedCount: 0,
            ConversationsTipRepaired: 0));
    }

    public Task<MessageRetentionPurgeableStats> GetPurgeableStatsAsync(
        long cutoffReceivedAtMs,
        CancellationToken ct = default) =>
        Task.FromResult(new MessageRetentionPurgeableStats(0, null));
}

using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// Noop tombstone 存储。测试与未配置 PostgreSQL 时使用。
/// IsUserDeletedAsync 始终返回 false，GetLifecycleStateAsync 始终返回 Active。
/// </summary>
public sealed class NoopUserDeletionTombstoneStore(ILogger<NoopUserDeletionTombstoneStore> logger)
    : IUserDeletionTombstoneStore
{
    public Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default)
    {
        logger.LogDebug("Noop tombstone check: user={UserId} assumed not-deleted", userId);
        return Task.FromResult(false);
    }

    public Task<UserLifecycleState> GetLifecycleStateAsync(long userId, CancellationToken ct = default)
    {
        logger.LogDebug("Noop lifecycle check: user={UserId} assumed active", userId);
        return Task.FromResult(UserLifecycleState.Active);
    }

    public Task RecordDeletionAsync(
        long userId,
        string deletionEventId,
        long deletedAtMs,
        CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop tombstone record skipped: user={UserId}; event={EventId}",
            userId,
            deletionEventId);
        return Task.CompletedTask;
    }

    public Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default)
    {
        logger.LogDebug("Noop tombstone completion skipped: user={UserId}", userId);
        return Task.CompletedTask;
    }

    public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default)
    {
        logger.LogDebug("Noop tombstone purge skipped: cutoff={Cutoff}", cutoffMs);
        return Task.FromResult(0L);
    }
}

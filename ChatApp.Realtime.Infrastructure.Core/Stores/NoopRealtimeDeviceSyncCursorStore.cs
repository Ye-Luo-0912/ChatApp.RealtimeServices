using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeDeviceSyncCursorStore : IRealtimeDeviceSyncCursorStore
{
    public Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
        long userId,
        ulong deviceIdHash,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<DeviceSyncCursor>>([]);
    }

    public Task UpsertManyAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<DeviceSyncCursor> cursors,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task DeleteAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<string> conversationIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(0L);
    }

    public Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(0L);
    }
}

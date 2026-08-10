using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Concurrency;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class RealtimeOutboxSignal :
    IRealtimeOutboxSignal,
    IRealtimeOutboxHintSource,
    IRealtimeOutboxPreclaimCoordinator,
    IDisposable
{
    private const int DefaultHintCapacity = 65_536;
    private readonly CoalescingAsyncSignal _signal = new();
    private readonly ConcurrentQueue<RealtimeOutboxHint> _committedHints = new();
    private readonly int _hintCapacity;
    private int _hintCount;
    private PreclaimConfiguration? _preclaimConfiguration;

    public RealtimeOutboxSignal(int hintCapacity = DefaultHintCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(hintCapacity);
        _hintCapacity = hintCapacity;
    }

    public void Notify() => _signal.Notify();

    public void Notify(string committedEventId)
    {
        if (!string.IsNullOrWhiteSpace(committedEventId) && TryReserveHintSlot())
            _committedHints.Enqueue(new RealtimeOutboxHint(committedEventId));

        // 即使队列已满也必须唤醒。发布器会在提示耗尽或恢复周期到期时扫描数据库。
        _signal.Notify();
    }

    public bool TryReadCommittedHint(out RealtimeOutboxHint hint)
    {
        if (_committedHints.TryDequeue(out hint))
        {
            Interlocked.Decrement(ref _hintCount);
            return true;
        }

        hint = default;
        return false;
    }

    public void ConfigurePreclaimOwner(string instanceId, TimeSpan leaseDuration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));

        // 同一个 publisher 生命周期内复用 claim token。event_id + token + lease 仍能阻止
        // 过期完成误删恢复后重新领取的行，同时避免为每条消息创建 GUID/string。
        Volatile.Write(
            ref _preclaimConfiguration,
            new PreclaimConfiguration(
                instanceId,
                Guid.NewGuid().ToString("N"),
                checked((long)leaseDuration.TotalMilliseconds)));
    }

    public bool TryReservePreclaim(string eventId, out RealtimeOutboxPreclaim preclaim)
    {
        var configuration = Volatile.Read(ref _preclaimConfiguration);
        if (string.IsNullOrWhiteSpace(eventId)
            || configuration is null
            || !TryReserveHintSlot())
        {
            preclaim = default;
            return false;
        }

        preclaim = new RealtimeOutboxPreclaim(
            eventId,
            configuration.Owner,
            configuration.ClaimToken,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + configuration.LeaseMilliseconds);
        return true;
    }

    public void CommitPreclaim(RealtimeOutboxPreclaim preclaim)
    {
        _committedHints.Enqueue(new RealtimeOutboxHint(
            preclaim.EventId,
            preclaim.LockOwner,
            preclaim.ClaimToken));
        _signal.Notify();
    }

    public void CancelPreclaim(RealtimeOutboxPreclaim preclaim) =>
        Interlocked.Decrement(ref _hintCount);

    public ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken ct = default) =>
        _signal.WaitAsync(timeout, ct);

    public void Dispose() => _signal.Dispose();

    private bool TryReserveHintSlot()
    {
        var count = Volatile.Read(ref _hintCount);
        while (count < _hintCapacity)
        {
            var observed = Interlocked.CompareExchange(ref _hintCount, count + 1, count);
            if (observed == count)
                return true;

            count = observed;
        }

        return false;
    }

    private sealed record PreclaimConfiguration(
        string Owner,
        string ClaimToken,
        long LeaseMilliseconds);
}

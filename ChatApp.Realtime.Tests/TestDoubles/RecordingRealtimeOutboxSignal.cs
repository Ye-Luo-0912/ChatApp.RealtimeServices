using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Tests.TestDoubles;

internal sealed class RecordingRealtimeOutboxSignal : IRealtimeOutboxSignal
{
    private int _notifications;

    public int Notifications => Volatile.Read(ref _notifications);

    public void Notify() => Interlocked.Increment(ref _notifications);

    public ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken ct = default) =>
        ValueTask.FromResult(false);
}
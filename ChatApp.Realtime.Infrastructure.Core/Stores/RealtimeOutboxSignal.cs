using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class RealtimeOutboxSignal : IRealtimeOutboxSignal, IDisposable
{
    private readonly SemaphoreSlim _signal = new(0, 1);

    public void Notify()
    {
        if (_signal.CurrentCount != 0)
            return;

        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    public ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken ct = default) =>
        new(_signal.WaitAsync(timeout, ct));

    public void Dispose() => _signal.Dispose();
}
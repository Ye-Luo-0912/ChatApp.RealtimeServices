namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeOutboxSignal
{
    void Notify();

    ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken ct = default);
}
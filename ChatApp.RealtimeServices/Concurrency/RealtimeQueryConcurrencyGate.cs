using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Concurrency;

/// <summary>
/// 五类查询 Worker 共享的数据库并发预算，避免各自 8 并发叠加成约 40 个循环。
/// </summary>
public sealed class RealtimeQueryConcurrencyGate
{
    private readonly SemaphoreSlim _semaphore;

    public RealtimeQueryConcurrencyGate(IOptions<RealtimeOptions> options)
    {
        var permits = Math.Max(1, options.Value.HistoryQueryConcurrency);
        _semaphore = new SemaphoreSlim(permits, permits);
        Permits = permits;
    }

    public int Permits { get; }

    public Task WaitAsync(CancellationToken ct) => _semaphore.WaitAsync(ct);

    public void Release() => _semaphore.Release();
}

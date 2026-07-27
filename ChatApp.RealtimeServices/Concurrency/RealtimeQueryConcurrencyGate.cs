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

    /// <summary>
    /// 过载协议：尝试在 <paramref name="timeoutMs"/> 内获取信号量。
    /// 成功返回 true；超时返回 false（调用方应回复 <c>server_busy</c>）。
    /// </summary>
    public async Task<bool> WaitAsync(int timeoutMs, CancellationToken ct)
    {
        if (timeoutMs <= 0)
        {
            await _semaphore.WaitAsync(ct).ConfigureAwait(false);
            return true;
        }
        return await _semaphore.WaitAsync(timeoutMs, ct).ConfigureAwait(false);
    }

    public void Release() => _semaphore.Release();
}

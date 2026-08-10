namespace ChatApp.Realtime.Infrastructure.Core.Concurrency;

/// <summary>
/// 线程安全的单槽异步信号。多个尚未消费的 <see cref="Notify"/> 会合并为一次唤醒，
/// 适合“状态已变化，请重新扫描”语义，不为每次通知创建 Task/CTS，也不依赖异常处理竞争。
/// </summary>
/// <remarks>
/// <para>
/// <c>_pending</c> 与信号量许可共同表示同一个单槽状态：只有从 0 变为 1 的线程才释放许可，
/// 因此信号量计数永远不会超过 1。
/// </para>
/// <para>
/// 等待者取得许可后先清除 <c>_pending</c>，再返回调用方执行状态扫描。清除前到达的通知可安全
/// 合并到本次扫描；清除后到达的通知会释放下一张许可，不会丢失后续工作。
/// </para>
/// </remarks>
public sealed class CoalescingAsyncSignal : IDisposable
{
    private readonly SemaphoreSlim _signal = new(0, 1);
    private int _pending;
    private int _disposed;

    /// <summary>尝试发布通知；已有待消费通知时直接合并。</summary>
    /// <returns>本次是否创建了新的待消费信号。</returns>
    public bool Notify()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Interlocked.Exchange(ref _pending, 1) != 0)
            return false;

        _signal.Release();
        return true;
    }

    public async ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var acquired = await _signal
            .WaitAsync(timeout, cancellationToken)
            .ConfigureAwait(false);
        if (!acquired)
            return false;

        Volatile.Write(ref _pending, 0);
        return true;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            _signal.Dispose();
    }
}

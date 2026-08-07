using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// Perf-6 / P0-6：跨分区共享的字节预算跟踪器。使用 <see cref="Interlocked"/> 实现无锁CAS。
/// 入队前通过 <see cref="AcquireAsync"/> 获取 <see cref="ByteBudgetLease"/>，
/// 处理完成（processor finally 块）时 Dispose lease 释放配额。
/// 预算为 0 或负数时表示不限制字节。
/// </summary>
/// <remarks>
/// P0-6 修复要点：
/// 1. <see cref="MaxSinglePayloadBytes"/> 硬上限：单条消息超过该值时永久拒绝（抛异常），避免永久轮询等待。
/// 2. lease 语义：配额在处理完成时释放，而非 Channel dequeue 时释放，预算覆盖 queued + processing。
/// 3. 异步信号：使用 <see cref="SemaphoreSlim"/> 替代 Task.Delay 轮询。
/// </remarks>
internal sealed class ByteBudget : IDisposable
{
    private readonly long _maxBytes;
    private readonly long _maxSinglePayloadBytes;
    private long _currentBytes;
    // P0-6：异步信号。Release lease 时 Release 信号量，唤醒一个等待的 AcquireAsync。
    // 初始为 0，等待者在 TryAcquire 失败后 WaitAsync 阻塞，避免 Task.Delay 轮询。
    private readonly SemaphoreSlim _signal = new(0, int.MaxValue);

    public ByteBudget(long maxBytes, long maxSinglePayloadBytes = 0)
    {
        _maxBytes = maxBytes;
        // P0-6：未指定单条硬上限时，默认等于总预算，避免单条消息超过总预算导致永久等待。
        _maxSinglePayloadBytes = maxSinglePayloadBytes > 0
            ? maxSinglePayloadBytes
            : (maxBytes > 0 ? maxBytes : 0);
    }

    public long CurrentBytes => Interlocked.Read(ref _currentBytes);
    public long MaxBytes => _maxBytes;
    public long MaxSinglePayloadBytes => _maxSinglePayloadBytes;

    /// <summary>
    /// P0-6：异步获取 <paramref name="bytes"/> 字节配额，返回 <see cref="ByteBudgetLease"/>。
    /// lease Dispose 时释放配额并唤醒等待者。
    /// 单条消息超过 <see cref="MaxSinglePayloadBytes"/> 时抛出
    /// <see cref="ByteBudgetOversizedException"/>（永久拒绝，调用方应转入死信）。
    /// </summary>
    public async ValueTask<ByteBudgetLease> AcquireAsync(long bytes, CancellationToken ct)
    {
        if (_maxBytes <= 0 || bytes <= 0)
            return new ByteBudgetLease(this, 0);

        // P0-6：硬上限检查——单条消息超过 MaxSinglePayloadBytes 时立即永久拒绝
        if (_maxSinglePayloadBytes > 0 && bytes > _maxSinglePayloadBytes)
            throw new ByteBudgetOversizedException(bytes, _maxSinglePayloadBytes);

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var lease = TryAcquireInternal(bytes);
            if (lease is not null)
                return lease;
            // P0-6：等待异步信号，替代 Task.Delay 轮询。
            // Release lease 时会 Release 信号量唤醒等待者；被唤醒后重新竞争。
            await _signal.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 同步尝试获取 <paramref name="bytes"/> 字节配额。成功返回 true；超预算或永久拒绝返回 false。
    /// </summary>
    /// <remarks>
    /// [Obsolete] 旧 API，不返回 lease，调用方需手动调用 <see cref="Release"/>。
    /// 新代码应使用 <see cref="AcquireAsync"/>。
    /// </remarks>
    [Obsolete("使用 AcquireAsync 获取 lease，lease Dispose 时自动释放配额。")]
    public bool TryAcquire(long bytes)
    {
        if (_maxBytes <= 0 || bytes <= 0)
            return true;
        // P0-6：硬上限检查——单条消息超过 MaxSinglePayloadBytes 时永久拒绝
        if (_maxSinglePayloadBytes > 0 && bytes > _maxSinglePayloadBytes)
            return false;
        return TryAcquireInternal(bytes) is not null;
    }

    /// <summary>
    /// P0-6：CAS 尝试获取配额。成功返回 lease；暂时不可获取返回 null。
    /// 当 bytes ≤ MaxSinglePayloadBytes 但 &gt; maxBytes 且当前无占用时，允许独占。
    /// </summary>
    private ByteBudgetLease? TryAcquireInternal(long bytes)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _currentBytes);
            var next = current + bytes;
            if (next > _maxBytes)
            {
                // P0-6：超预算，但 ≤ MaxSinglePayloadBytes 且当前无占用（队列为空）时，允许独占
                if (current == 0 && _maxSinglePayloadBytes > 0 && bytes <= _maxSinglePayloadBytes)
                {
                    if (Interlocked.CompareExchange(ref _currentBytes, bytes, 0) == 0)
                        return new ByteBudgetLease(this, bytes);
                    continue; // CAS 失败，重试
                }
                return null; // 暂时不可获取，等待者应等待信号
            }
            if (Interlocked.CompareExchange(ref _currentBytes, next, current) == current)
                return new ByteBudgetLease(this, bytes);
            // CAS 失败，重试
        }
    }

    /// <summary>释放已处理的字节数（旧 API，供 [Obsolete] 调用方使用）。</summary>
    [Obsolete("使用 ByteBudgetLease.DisposeAsync 释放配额。")]
    public void Release(long bytes)
    {
        if (_maxBytes <= 0 || bytes <= 0)
            return;
        Interlocked.Add(ref _currentBytes, -bytes);
        SignalWaiters();
    }

    /// <summary>P0-6：lease 释放时调用，归还占用的字节配额并唤醒等待者。</summary>
    internal void ReleaseLease(long bytes)
    {
        if (bytes <= 0)
            return;
        Interlocked.Add(ref _currentBytes, -bytes);
        SignalWaiters();
    }

    /// <summary>P0-6：唤醒一个等待的 AcquireAsync。使用 SemaphoreSlim.Release 释放一个许可。</summary>
    private void SignalWaiters()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
            // 信号量计数已达上限（理论上极少发生），忽略。
            // 等待者会在下次 TryAcquire 时检测到 _currentBytes 变化。
        }
    }

    public void Dispose() => _signal.Dispose();
}

/// <summary>
/// P0-6：字节预算 lease。Dispose 时归还占用的字节配额并唤醒等待者。
/// 预算覆盖 queued + processing，lease 在处理完成（processor finally 块）时释放，
/// 而非在 Channel dequeue 时释放，确保正在执行的大消息始终计入内存预算。
/// </summary>
internal sealed class ByteBudgetLease : IAsyncDisposable, IDisposable
{
    private readonly ByteBudget _budget;
    private readonly long _bytes;
    private int _disposed;

    internal ByteBudgetLease(ByteBudget budget, long bytes)
    {
        _budget = budget;
        _bytes = bytes;
    }

    public long Bytes => _bytes;
    public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            _budget.ReleaseLease(_bytes);
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// P0-6：单条消息超过 <see cref="ByteBudget.MaxSinglePayloadBytes"/> 时抛出。
/// 调用方应捕获并将消息转入死信流，而非无限等待预算。
/// </summary>
internal sealed class ByteBudgetOversizedException : ArgumentException
{
    public long RequestedBytes { get; }
    public long MaxSinglePayloadBytes { get; }

    public ByteBudgetOversizedException(long requestedBytes, long maxSinglePayloadBytes)
        : base($"单条消息字节数 {requestedBytes} 超过硬上限 {maxSinglePayloadBytes}，永久拒绝。")
    {
        RequestedBytes = requestedBytes;
        MaxSinglePayloadBytes = maxSinglePayloadBytes;
    }
}

/// <summary>
/// P0-6：包装 envelope 与对应的 <see cref="ByteBudgetLease"/>。
/// lease 在 processor finally 块中 Dispose 释放，预算覆盖 queued + processing。
/// 未启用字节预算时 Lease 为 null。
/// </summary>
internal readonly struct LeasedEnvelope<T>
{
    public T Envelope { get; }
    public ByteBudgetLease? Lease { get; }
    /// <summary>
    /// Reliability-4：ACK 租约，在 envelope 从 NATS 收到后立即注册（ProduceAsync），
    /// 覆盖排队等待 + 处理全周期。processor finally 块调用 <see cref="AckLease.Complete"/>
    /// 停止 progress-ack（轻量原子置位，无异步清理）。
    /// </summary>
    public AckLease AckLease { get; }

    public LeasedEnvelope(T envelope, ByteBudgetLease? lease, AckLease ackLease = default)
    {
        Envelope = envelope;
        Lease = lease;
        AckLease = ackLease;
    }
}

/// <summary>
/// Perf-6（已废弃）：包装 <see cref="ChannelReader{T}"/>，在每次 <see cref="TryRead"/> 成功后
/// 释放对应字节到共享 <see cref="ByteBudget"/>。已被 lease 语义（<see cref="LeasedEnvelope{T}"/>
/// + <see cref="ByteBudgetLease"/>）取代：lease 在 processor finally 块释放，预算覆盖 processing。
/// </summary>
[Obsolete("使用 LeasedEnvelope<T> + ByteBudgetLease 替代。lease 在 processor finally 块释放。")]
internal sealed class ByteReleasingChannelReader<T> : ChannelReader<T>
{
    private readonly ChannelReader<T> _inner;
    private readonly ByteBudget _budget;
    private readonly Func<T, long> _sizer;

    public ByteReleasingChannelReader(
        ChannelReader<T> inner,
        ByteBudget budget,
        Func<T, long> sizer)
    {
        _inner = inner;
        _budget = budget;
        _sizer = sizer;
    }

    public override bool CanCount => _inner.CanCount;
    public override bool CanPeek => _inner.CanPeek;
    public override int Count => _inner.Count;

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _inner.WaitToReadAsync(cancellationToken);

    public override bool TryRead(out T item)
    {
        if (!_inner.TryRead(out item!))
            return false;
        _budget.Release(_sizer(item));
        return true;
    }
}

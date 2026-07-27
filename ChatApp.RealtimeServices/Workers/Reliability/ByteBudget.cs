using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// Perf-6：跨分区共享的字节预算跟踪器。使用 <see cref="Interlocked"/> 实现无锁CAS。
/// 入队前 <see cref="TryAcquire"/>，处理完成后 <see cref="Release"/>。
/// 预算为 0 或负数时表示不限制字节。
/// </summary>
internal sealed class ByteBudget
{
    private readonly long _maxBytes;
    private long _currentBytes;

    public ByteBudget(long maxBytes)
    {
        _maxBytes = maxBytes;
    }

    public long CurrentBytes => Interlocked.Read(ref _currentBytes);

    /// <summary>
    /// 尝试获取 <paramref name="bytes"/> 字节配额。成功返回 true；超预算返回 false。
    /// </summary>
    public bool TryAcquire(long bytes)
    {
        if (_maxBytes <= 0)
            return true;
        if (bytes <= 0)
            return true;

        while (true)
        {
            var current = Interlocked.Read(ref _currentBytes);
            var next = current + bytes;
            if (next > _maxBytes)
                return false;
            if (Interlocked.CompareExchange(ref _currentBytes, next, current) == current)
                return true;
        }
    }

    /// <summary>释放已处理的字节数。</summary>
    public void Release(long bytes)
    {
        if (_maxBytes <= 0 || bytes <= 0)
            return;
        Interlocked.Add(ref _currentBytes, -bytes);
    }
}

/// <summary>
/// Perf-6：包装 <see cref="ChannelReader{T}"/>，在每次 <see cref="TryRead"/> 成功后
/// 释放对应字节到共享 <see cref="ByteBudget"/>。<see cref="ReadAllAsync"/> 的默认实现
/// 基于 <see cref="WaitToReadAsync"/> + <see cref="TryRead"/>，因此也会自动释放。
/// </summary>
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

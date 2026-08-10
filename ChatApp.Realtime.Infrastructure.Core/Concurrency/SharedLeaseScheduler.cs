using System.Diagnostics;

namespace ChatApp.Realtime.Infrastructure.Core.Concurrency;

/// <summary>
/// 用单个定时器为同类短生命周期操作共享续租调度，避免每个操作各自创建
/// <see cref="Task"/>、<see cref="CancellationTokenSource"/> 和定时器。
/// </summary>
/// <typeparam name="TState">续租回调需要的状态；结构体状态可避免额外堆分配。</typeparam>
public sealed class SharedLeaseScheduler<TState> : IAsyncDisposable
{
    private const int MaximumRetainedNodes = 4096;

    public delegate ValueTask<bool> RenewLeaseAsync(
        TState state,
        CancellationToken schedulerStoppingToken);

    private readonly object _gate = new();
    private readonly List<SharedLeaseNode<TState>> _pending = [];
    private readonly Stack<SharedLeaseNode<TState>> _pool = [];
    private readonly long _renewalIntervalTicks;
    private readonly PeriodicTimer _timer;
    private readonly CancellationTokenSource _stoppingCts = new();
    private readonly RenewLeaseAsync _renewLeaseAsync;
    private readonly Action<Exception>? _renewalError;
    private readonly Task _loopTask;
    private TaskCompletionSource<bool>? _inFlightDrained;
    private int _inFlight;
    private bool _disposed;

    public SharedLeaseScheduler(
        TimeSpan renewalInterval,
        RenewLeaseAsync renewLeaseAsync,
        Action<Exception>? renewalError = null)
    {
        if (renewalInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(renewalInterval));

        ArgumentNullException.ThrowIfNull(renewLeaseAsync);
        _renewLeaseAsync = renewLeaseAsync;
        _renewalError = renewalError;
        _renewalIntervalTicks = Math.Max(
            1L,
            checked((long)Math.Ceiling(renewalInterval.TotalSeconds * Stopwatch.Frequency)));
        var tickInterval = renewalInterval < TimeSpan.FromSeconds(1)
            ? renewalInterval
            : TimeSpan.FromSeconds(1);
        _timer = new PeriodicTimer(tickInterval);
        _loopTask = RunLoopAsync(_stoppingCts.Token);
    }

    /// <summary>
    /// 注册一个租约。完成时从可索引最小堆移除，节点会立即进入有界共享池复用。
    /// </summary>
    public SharedLeaseRegistration<TState> Register(
        TState state,
        CancellationToken operationToken = default)
    {
        lock (_gate)
        {
            if (_disposed)
                return default;

            var node = _pool.Count > 0
                ? _pool.Pop()
                : new SharedLeaseNode<TState>();

            var version = node.Initialize(this, state, operationToken, _renewalIntervalTicks);
            EnqueueLocked(node);
            return new SharedLeaseRegistration<TState>(node, version);
        }
    }

    private async Task RunLoopAsync(CancellationToken stoppingToken)
    {
        var due = new List<(SharedLeaseNode<TState> Node, long Version)>();
        try
        {
            while (await _timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                due.Clear();
                var now = Stopwatch.GetTimestamp();
                lock (_gate)
                {
                    while (_pending.Count > 0 && _pending[0].DueAtTicks <= now)
                    {
                        var node = DequeueRootLocked();
                        var version = node.Version;
                        if (node.IsCompleted(version) || node.OperationToken.IsCancellationRequested)
                        {
                            node.MarkCompleted(version);
                            ReturnNodeLocked(node, version);
                            continue;
                        }

                        node.SetRenewing(version, renewing: true);
                        _inFlight++;
                        due.Add((node, version));
                    }
                }

                for (var i = 0; i < due.Count; i++)
                {
                    var item = due[i];
                    _ = RenewAndRescheduleAsync(item.Node, item.Version, now, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // 正常停止。
        }
    }

    private async Task RenewAndRescheduleAsync(
        SharedLeaseNode<TState> node,
        long version,
        long firedAt,
        CancellationToken stoppingToken)
    {
        var renewalAccepted = true;
        try
        {
            renewalAccepted = await _renewLeaseAsync(node.State, stoppingToken).ConfigureAwait(false);
            if (!renewalAccepted)
                node.MarkRenewalRejected(version);
        }
        catch (OperationCanceledException) when (
            stoppingToken.IsCancellationRequested || node.OperationToken.IsCancellationRequested)
        {
            node.MarkCompleted(version);
        }
        catch (Exception ex)
        {
            // 暂时性续租失败沿用原语义：记录后继续调度。只有回调明确返回 false 才判定租约失效。
            try
            {
                _renewalError?.Invoke(ex);
            }
            catch
            {
                // 诊断回调不得破坏调度器的 in-flight 记账和回收。
            }
        }

        var returnNode = false;
        lock (_gate)
        {
            _inFlight--;
            node.SetRenewing(version, renewing: false);
            if (_disposed
                || !renewalAccepted
                || node.IsCompleted(version)
                || node.OperationToken.IsCancellationRequested)
            {
                // renewalAccepted=false 时节点保留到调用方 Complete，以便调用方读取 RenewalRejected。
                returnNode = renewalAccepted || node.IsCompleted(version) || _disposed;
            }
            else
            {
                node.Reschedule(version, firedAt, _renewalIntervalTicks);
                EnqueueLocked(node);
            }

            if (_inFlight == 0)
                _inFlightDrained?.TrySetResult(true);

            if (returnNode)
                ReturnNodeLocked(node, version);
        }
    }

    internal void Complete(SharedLeaseNode<TState> node, long version)
    {
        lock (_gate)
        {
            if (!node.MarkCompleted(version))
                return;

            // 排队中的节点可从可索引最小堆直接移除并立即复用；正在续租的节点必须等
            // 回调退出后再回收，避免下一份租约覆盖仍在读取的 State。
            if (node.HeapIndex >= 0)
            {
                RemoveAtLocked(node.HeapIndex);
                ReturnNodeLocked(node, version);
            }
            else if (!node.IsRenewing(version))
            {
                // 续租明确拒绝后节点不再位于堆中，由调用方 Complete 归还。
                ReturnNodeLocked(node, version);
            }
        }
    }

    private void ReturnNodeLocked(SharedLeaseNode<TState> node, long version)
    {
        node.MarkCompleted(version);
        if (!node.TryReset(version))
            return;
        if (!_disposed && _pool.Count < MaximumRetainedNodes)
            _pool.Push(node);
    }

    private void EnqueueLocked(SharedLeaseNode<TState> node)
    {
        node.HeapIndex = _pending.Count;
        _pending.Add(node);
        SiftUpLocked(node.HeapIndex);
    }

    private SharedLeaseNode<TState> DequeueRootLocked()
    {
        var node = _pending[0];
        RemoveAtLocked(0);
        return node;
    }

    private void RemoveAtLocked(int index)
    {
        var removed = _pending[index];
        var lastIndex = _pending.Count - 1;
        var last = _pending[lastIndex];
        _pending.RemoveAt(lastIndex);
        removed.HeapIndex = -1;
        if (index == lastIndex)
            return;

        _pending[index] = last;
        last.HeapIndex = index;
        if (index > 0 && IsEarlier(last, _pending[(index - 1) / 2]))
            SiftUpLocked(index);
        else
            SiftDownLocked(index);
    }

    private void SiftUpLocked(int index)
    {
        while (index > 0)
        {
            var parentIndex = (index - 1) / 2;
            if (!IsEarlier(_pending[index], _pending[parentIndex]))
                break;
            SwapLocked(index, parentIndex);
            index = parentIndex;
        }
    }

    private void SiftDownLocked(int index)
    {
        while (true)
        {
            var left = (index * 2) + 1;
            if (left >= _pending.Count)
                return;
            var right = left + 1;
            var earliest = right < _pending.Count && IsEarlier(_pending[right], _pending[left])
                ? right
                : left;
            if (!IsEarlier(_pending[earliest], _pending[index]))
                return;
            SwapLocked(index, earliest);
            index = earliest;
        }
    }

    private void SwapLocked(int first, int second)
    {
        (_pending[first], _pending[second]) = (_pending[second], _pending[first]);
        _pending[first].HeapIndex = first;
        _pending[second].HeapIndex = second;
    }

    private static bool IsEarlier(
        SharedLeaseNode<TState> left,
        SharedLeaseNode<TState> right) => left.DueAtTicks < right.DueAtTicks;

    public async ValueTask DisposeAsync()
    {
        List<(SharedLeaseNode<TState> Node, long Version)> queued;
        Task? inFlightTask = null;
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;
            queued = new List<(SharedLeaseNode<TState>, long)>(_pending.Count);
            while (_pending.Count > 0)
            {
                var node = DequeueRootLocked();
                queued.Add((node, node.Version));
            }

            if (_inFlight > 0)
            {
                _inFlightDrained = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                inFlightTask = _inFlightDrained.Task;
            }
        }

        _stoppingCts.Cancel();
        _timer.Dispose();
        await _loopTask.ConfigureAwait(false);

        lock (_gate)
        {
            for (var i = 0; i < queued.Count; i++)
                ReturnNodeLocked(queued[i].Node, queued[i].Version);
            _pool.Clear();
        }

        if (inFlightTask is not null)
            await inFlightTask.ConfigureAwait(false);

        _stoppingCts.Dispose();
    }
}

/// <summary>共享调度器返回的轻量租约句柄。</summary>
public readonly struct SharedLeaseRegistration<TState>
{
    private readonly SharedLeaseNode<TState>? _node;
    private readonly long _version;

    internal SharedLeaseRegistration(SharedLeaseNode<TState> node, long version)
    {
        _node = node;
        _version = version;
    }

    public bool IsActive => _node is not null
                            && !_node.IsCompleted(_version)
                            && !_node.IsRenewalRejected(_version);

    public bool RenewalRejected => _node?.IsRenewalRejected(_version) == true;

    public void Complete() => _node?.Owner?.Complete(_node, _version);
}

internal sealed class SharedLeaseNode<TState>
{
    private long _version;
    private int _completed;
    private int _renewalRejected;
    private int _renewing;
    private int _pooled = 1;
    private long _dueAtTicks;

    internal SharedLeaseScheduler<TState>? Owner { get; private set; }
    internal TState State { get; private set; } = default!;
    internal CancellationToken OperationToken { get; private set; }
    internal long Version => Volatile.Read(ref _version);
    internal long DueAtTicks => Volatile.Read(ref _dueAtTicks);
    internal int HeapIndex { get; set; } = -1;

    internal long Initialize(
        SharedLeaseScheduler<TState> owner,
        TState state,
        CancellationToken operationToken,
        long renewalIntervalTicks)
    {
        Owner = owner;
        State = state;
        OperationToken = operationToken;
        Volatile.Write(ref _completed, 0);
        Volatile.Write(ref _renewalRejected, 0);
        Volatile.Write(ref _renewing, 0);
        Volatile.Write(ref _pooled, 0);
        HeapIndex = -1;
        var version = Interlocked.Increment(ref _version);
        Volatile.Write(ref _dueAtTicks, Stopwatch.GetTimestamp() + renewalIntervalTicks);
        return version;
    }

    internal bool IsCompleted(long version) =>
        Version != version || Volatile.Read(ref _completed) != 0;

    internal bool MarkCompleted(long version)
    {
        if (Version != version)
            return false;
        return Interlocked.Exchange(ref _completed, 1) == 0;
    }

    internal void MarkRenewalRejected(long version)
    {
        if (Version == version)
            Volatile.Write(ref _renewalRejected, 1);
    }

    internal bool IsRenewalRejected(long version) =>
        Version == version && Volatile.Read(ref _renewalRejected) != 0;

    internal bool IsRenewing(long version) =>
        Version == version && Volatile.Read(ref _renewing) != 0;

    internal void SetRenewing(long version, bool renewing)
    {
        if (Version == version)
            Volatile.Write(ref _renewing, renewing ? 1 : 0);
    }

    internal void Reschedule(long version, long firedAt, long renewalIntervalTicks)
    {
        if (Version == version)
            Volatile.Write(ref _dueAtTicks, firedAt + renewalIntervalTicks);
    }

    internal bool TryReset(long version)
    {
        if (Version != version || Interlocked.Exchange(ref _pooled, 1) != 0)
            return false;

        Owner = null;
        State = default!;
        OperationToken = default;
        HeapIndex = -1;
        return true;
    }
}

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// Reliability-4 的共享 ACK 租约调度器：用单个定时器 + 小顶堆为所有正在进行中的
/// JetStream 消息统一管理 In-Progress ACK（WPI），替代原先"每条消息一个
/// ProgressAckGuard（CTS + PeriodicTimer + Task）"的高开销方案。
/// </summary>
/// <remarks>
/// 快路径优化：消息入队时仅做 <c>register(deliveryHandle, dueAt = now + AckWait/2)</c>，
/// 不启动任何 Timer/Task。只有排队+处理时间真正接近 <c>AckWait/2</c> 时，调度器才
/// 发送一次 progress-ack 并向后续租。因此快速完成的消息成本从
/// "Task + CTS + Timer + OperationCanceledException" 降到 "slot registration + timestamp"。
/// 活跃 lease 数天然受 <see cref="MaxAckPending"/> / 队列容量约束。
/// </remarks>
internal sealed class AckLeaseScheduler : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly PriorityQueue<AckLeaseNode, long> _pending = new();
    private readonly TimeSpan _leaseInterval;
    private readonly TimeSpan _tickInterval;
    private readonly PeriodicTimer _timer;
    private readonly Task _loopTask;
    private readonly CancellationTokenSource _cts;
    private readonly ILogger _logger;
    private bool _disposed;

    private AckLeaseScheduler(
        TimeSpan ackWait,
        ILogger logger)
    {
        _logger = logger;
        _leaseInterval = JetStreamAckTiming.GetProgressAckInterval(ackWait);
        // 定时器刻度取 AckWait/2 与 1s 的较小值，保证最坏情况下 progress-ack
        // 仍在 AckWait 到期前发出（避免刻度与入队时刻错位导致续租滞后）。
        _tickInterval = _leaseInterval < TimeSpan.FromSeconds(1)
            ? _leaseInterval
            : TimeSpan.FromSeconds(1);
        _cts = new CancellationTokenSource();
        _timer = new PeriodicTimer(_tickInterval);
        _loopTask = RunLoopAsync(_cts.Token);
    }

    /// <summary>
    /// 启动共享调度器。AckWait &lt;= 0 时返回 null（无意义）。
    /// 每个 worker runtime 只应创建一个实例。
    /// </summary>
    public static AckLeaseScheduler? Start(
        TimeSpan ackWait,
        ILogger logger) =>
        ackWait <= TimeSpan.Zero
            ? null
            : new AckLeaseScheduler(ackWait, logger);

    /// <summary>
    /// 为一条消息注册 ACK 租约。返回轻量 <see cref="AckLease"/>，
    /// 消息完成时调用 <see cref="AckLease.Complete"/> 即可，无需等待任何异步清理。
    /// 若调度器已终止则返回默认（空）租约。
    /// </summary>
    public AckLease Register(
        Func<CancellationToken, ValueTask> progressAck,
        CancellationToken processingToken)
    {
        var node = new AckLeaseNode(progressAck, processingToken, _leaseInterval);
        node.ArmAt(Stopwatch.GetTimestamp());
        lock (_gate)
        {
            if (_disposed)
                return default;
            _pending.Enqueue(node, node.DueAtTicks);
        }
        return new AckLease(node);
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                var now = Stopwatch.GetTimestamp();
                List<AckLeaseNode>? due = null;
                lock (_gate)
                {
                    while (_pending.TryPeek(out var node, out var dueAtTicks) &&
                           dueAtTicks <= now)
                    {
                        _pending.Dequeue();
                        (due ??= new List<AckLeaseNode>()).Add(node);
                    }
                }

                if (due is null)
                    continue;

                foreach (var node in due)
                {
                    if (node.IsCanceled)
                        continue;
                    // 每条到期的租约各自续租，避免单个慢 ACK 阻塞其余租约。
                    _ = FireAndRescheduleAsync(node, now, ct);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // 正常停止。
        }
    }

    private async Task FireAndRescheduleAsync(
        AckLeaseNode node,
        long firedAt,
        CancellationToken ct)
    {
        try
        {
            await node.ProgressAck(node.ProcessingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (node.ProcessingToken.IsCancellationRequested)
        {
            // 处理已取消，不再续租。
        }
        catch (Exception ex)
        {
            // progress-ack 失败不中断处理；AckWait 仍可能触发重投。
            _logger.LogDebug(ex, "In-Progress ACK 失败，AckWait 可能触发重投。");
        }

        if (node.IsCanceled)
            return;
        lock (_gate)
        {
            if (!_disposed && !node.IsCanceled)
            {
                node.Reschedule(firedAt);
                _pending.Enqueue(node, node.DueAtTicks);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _disposed = true;
        }
        _cts.Cancel();
        _timer.Dispose();
        return RunLoopToCompletionAsync();
    }

    private async ValueTask RunLoopToCompletionAsync()
    {
        try
        {
            await _loopTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // 预期行为。
        }
        finally
        {
            _cts.Dispose();
        }
    }
}

/// <summary>
/// 进程内跨线程传递的轻量 ACK 租约句柄。持有可变 <see cref="AckLeaseNode"/> 引用，
/// <c>Complete</c> 仅需一次原子置位，绝不触发异步清理或异常。
/// </summary>
internal readonly struct AckLease(AckLeaseNode? node)
{
    public bool IsActive => node is not null && !node.IsCanceled;

    /// <summary>
    /// 标记消息完成（无论快慢）。从 processor finally 块同步调用，无 alloc、无 await。
    /// </summary>
    public void Complete() => node?.Cancel();
}

/// <summary>
/// 调度器内部的可变租约节点。IsCanceled 采用 volatile 置位，与调度线程无锁协作。
/// </summary>
internal sealed class AckLeaseNode(
    Func<CancellationToken, ValueTask> progressAck,
    CancellationToken processingToken,
    TimeSpan leaseInterval)
{
    private int _canceled;
    private long _dueAtTicks;
    private readonly long _intervalTicks = Math.Max(
        1L,
        leaseInterval.Ticks * Stopwatch.Frequency / TimeSpan.TicksPerSecond);

    public Func<CancellationToken, ValueTask> ProgressAck { get; } = progressAck;
    public CancellationToken ProcessingToken { get; } = processingToken;

    public long DueAtTicks => Volatile.Read(ref _dueAtTicks);

    public bool IsCanceled => Volatile.Read(ref _canceled) != 0;

    /// <summary>
    /// 设置首次到期时间 = now + AckWait/2。必须在入堆前调用一次。
    /// </summary>
    public void ArmAt(long now)
    {
        Volatile.Write(ref _dueAtTicks, now + _intervalTicks);
    }

    public void Reschedule(long firedAt)
    {
        Volatile.Write(ref _dueAtTicks, firedAt + _intervalTicks);
    }

    public void Cancel() => Volatile.Write(ref _canceled, 1);
}
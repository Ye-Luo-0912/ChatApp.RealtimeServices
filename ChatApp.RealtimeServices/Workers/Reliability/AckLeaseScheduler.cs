using ChatApp.Realtime.Infrastructure.Core.Concurrency;
using Microsoft.Extensions.Logging;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// Reliability-4 的 ACK 租约适配器。实际调度由共享的
/// <see cref="SharedLeaseScheduler{TState}"/> 完成；每个 worker runtime 仅持有一个定时器。
/// </summary>
internal sealed class AckLeaseScheduler : IAsyncDisposable
{
    private readonly SharedLeaseScheduler<AckLeaseState> _scheduler;

    private AckLeaseScheduler(TimeSpan ackWait, ILogger logger)
    {
        _scheduler = new SharedLeaseScheduler<AckLeaseState>(
            JetStreamAckTiming.GetProgressAckInterval(ackWait),
            RenewAsync,
            ex => logger.LogDebug(ex, "In-Progress ACK 失败，AckWait 可能触发重投。"));
    }

    public static AckLeaseScheduler? Start(TimeSpan ackWait, ILogger logger) =>
        ackWait <= TimeSpan.Zero
            ? null
            : new AckLeaseScheduler(ackWait, logger);

    public AckLease Register(
        Func<CancellationToken, ValueTask> progressAck,
        CancellationToken processingToken)
    {
        ArgumentNullException.ThrowIfNull(progressAck);
        var state = new AckLeaseState(progressAck, processingToken);
        return new AckLease(_scheduler.Register(state, processingToken));
    }

    private static async ValueTask<bool> RenewAsync(
        AckLeaseState state,
        CancellationToken schedulerStoppingToken)
    {
        schedulerStoppingToken.ThrowIfCancellationRequested();
        await state.ProgressAck(state.ProcessingToken).ConfigureAwait(false);
        return true;
    }

    public ValueTask DisposeAsync() => _scheduler.DisposeAsync();

    internal readonly record struct AckLeaseState(
        Func<CancellationToken, ValueTask> ProgressAck,
        CancellationToken ProcessingToken);
}

/// <summary>
/// 进程内跨线程传递的轻量 ACK 租约句柄。完成快路径从共享调度堆移除节点并立即复用，
/// 不创建独立 Task、CTS 或定时器。
/// </summary>
internal readonly struct AckLease(
    SharedLeaseRegistration<AckLeaseScheduler.AckLeaseState> registration)
{
    public bool IsActive => registration.IsActive;

    public void Complete() => registration.Complete();
}

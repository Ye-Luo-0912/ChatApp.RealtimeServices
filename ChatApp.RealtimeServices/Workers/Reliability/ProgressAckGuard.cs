using Microsoft.Extensions.Logging;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// Reliability-4：在业务处理期间定期发送 JetStream In-Progress ACK（WPI），
/// 重置 AckWait 计时器，防止合法长时处理消息在完成前被 JetStream 重投。
/// </summary>
/// <remarks>
/// 使用方式：在处理单条消息时用 <c>using var guard = ProgressAckGuard.Start(...)</c> 包裹，
/// 处理完成（正常返回或异常）后 Dispose 停止计时器。
/// 计时器间隔为 <c>ackWait / 2</c>，确保在 AckWait 到期前至少发送一次 progress-ack。
/// </remarks>
internal sealed class ProgressAckGuard : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _progressAck;
    private readonly CancellationToken _cancellationToken;
    private readonly ILogger _logger;
    private readonly PeriodicTimer _timer;
    private readonly Task _loopTask;
    private readonly CancellationTokenSource _loopCts;

    private ProgressAckGuard(
        Func<CancellationToken, ValueTask> progressAck,
        TimeSpan interval,
        CancellationToken cancellationToken,
        ILogger logger)
    {
        _progressAck = progressAck;
        _cancellationToken = cancellationToken;
        _logger = logger;
        _loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _timer = new PeriodicTimer(interval);
        _loopTask = RunProgressAckLoopAsync(_loopCts.Token);
    }

    /// <summary>
    /// 启动 progress-ack 守卫。处理完成时 Dispose 停止计时器。
    /// </summary>
    /// <param name="progressAck">In-Progress ACK 回调（重置 JetStream AckWait）。</param>
    /// <param name="ackWait">JetStream consumer 的 AckWait 时长。</param>
    /// <param name="cancellationToken">处理取消令牌。</param>
    /// <param name="logger">日志器。</param>
    public static ProgressAckGuard? Start(
        Func<CancellationToken, ValueTask> progressAck,
        TimeSpan ackWait,
        CancellationToken cancellationToken,
        ILogger logger)
    {
        // AckWait <= 0 时不启动守卫（无意义）。
        if (ackWait <= TimeSpan.Zero)
            return null;

        // 间隔 = AckWait / 2，确保在 AckWait 到期前至少发送一次 progress-ack。
        // 最小间隔 5 秒，避免高频 ACK 给 NATS 带来不必要负载。
        var interval = TimeSpan.FromMilliseconds(
            Math.Max(5_000, ackWait.TotalMilliseconds / 2));
        return new ProgressAckGuard(progressAck, interval, cancellationToken, logger);
    }

    private async Task RunProgressAckLoopAsync(CancellationToken ct)
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await _progressAck(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // progress-ack 失败不中断处理流程；AckWait 仍可能触发重投。
                    _logger.LogDebug(ex, "In-Progress ACK 失败，AckWait 可能触发重投。");
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // 正常停止。
        }
    }

    public async ValueTask DisposeAsync()
    {
        _loopCts.Cancel();
        _timer.Dispose();
        try
        {
            await _loopTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // 预期行为。
        }
        _loopCts.Dispose();
    }
}

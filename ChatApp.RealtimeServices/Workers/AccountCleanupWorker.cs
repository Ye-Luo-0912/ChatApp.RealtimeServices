using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 消费 Realtime 域事件并执行账号删除清理（非网关推送路径）。
/// </summary>
public sealed class AccountCleanupWorker : BackgroundService
{
    private const string WorkerName = nameof(AccountCleanupWorker);

    private readonly IRealtimeEventConsumer _consumer;
    private readonly IUserAccountDeletedProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly IOptions<RealtimeOptions> _options;
    private readonly ILogger<AccountCleanupWorker> _logger;

    public AccountCleanupWorker(
        IRealtimeEventConsumer consumer,
        IUserAccountDeletedProcessor processor,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        ILogger<AccountCleanupWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "账号清理工作器已启动。消费者={Consumer}；处理器={Processor}",
            _consumer.GetType().Name,
            _processor.GetType().Name);

        _readinessState.MarkStarted(WorkerName);
        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);

        try
        {
            await foreach (var envelope in _consumer.ConsumeAsync(stoppingToken).ConfigureAwait(false))
            {
                _readinessState.MarkHeartbeat(WorkerName);
                var evt = envelope.Event;

                if (evt.Type == RealtimeEventType.AccountCleanupCompleted)
                {
                    // 完成事件由其它订阅方（Server Saga）处理；清理 worker 直接 ACK。
                    await envelope.AckAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (evt.Type != RealtimeEventType.UserAccountDeleted)
                {
                    // 同 subject 上的其它事件：ACK 跳过，不阻塞清理队列。
                    await envelope.AckAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                try
                {
                    var result = await _processor.ProcessAsync(evt, stoppingToken).ConfigureAwait(false);
                    if (result.Succeeded)
                    {
                        await envelope.AckAsync(stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    _logger.LogWarning(
                        "账号清理失败。事件={EventId}；错误={ErrorCode}；投递次数={DeliveryCount}",
                        evt.EventId,
                        result.ErrorCode,
                        envelope.DeliveryCount);

                    // 瞬时失败 NAK；永久失败也 ACK 避免毒丸（错误码不含 transient 时）
                    if (string.Equals(result.ErrorCode, "cleanup_transient", StringComparison.Ordinal))
                        await envelope.NakAsync(stoppingToken).ConfigureAwait(false);
                    else
                        await envelope.AckAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "账号清理处理异常，等待重投。事件={EventId}", evt.EventId);
                    try
                    {
                        await envelope.NakAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (Exception nakEx)
                    {
                        _logger.LogError(nakEx, "账号清理 NAK 失败。事件={EventId}", evt.EventId);
                    }
                }
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                _readinessState.MarkHeartbeat(WorkerName);
                await Task.Delay(_options.Value.WorkerIntervalMs, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("账号清理工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "账号清理工作器异常退出。");
            throw;
        }
        finally
        {
            heartbeatCts.Cancel();
            await SuppressCancellationAsync(heartbeatTask).ConfigureAwait(false);
            _readinessState.MarkStopped(WorkerName);
            _logger.LogInformation("账号清理工作器已停止。");
        }
    }

    private async Task RunHeartbeatAsync(CancellationToken ct)
    {
        var intervalMs = Math.Clamp(_options.Value.WorkerIntervalMs, 1000, 5000);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(intervalMs));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            _readinessState.MarkHeartbeat(WorkerName);
    }

    private static async Task SuppressCancellationAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }
}

using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class RealtimeEventWorker : BackgroundService
{
    private const string WorkerName = nameof(RealtimeEventWorker);

    private readonly IRealtimeEventConsumer _consumer;
    private readonly RealtimeReadinessState _readinessState;
    private readonly IOptions<RealtimeOptions> _options;
    private readonly ILogger<RealtimeEventWorker> _logger;

    public RealtimeEventWorker(
        IRealtimeEventConsumer consumer,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        ILogger<RealtimeEventWorker> logger)
    {
        _consumer = consumer;
        _readinessState = readinessState;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// 执行异步任务，启动实时事件处理流程。
    /// 该方法在后台服务启动时被调用，并持续运行直到接收到停止信号。
    /// </summary>
    /// <param name="stoppingToken">用于请求取消操作的令牌。当此令牌触发时，当前正在执行的操作应该被取消。</param>
    /// <returns>返回一个表示异步操作的任务。</returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "实时事件工作器已启动。消费者实现={Consumer}",
            _consumer.GetType().Name);

        _readinessState.MarkStarted(WorkerName);

        try
        {
            await foreach (var evt in _consumer.ConsumeAsync(stoppingToken).ConfigureAwait(false))
            {
                _readinessState.MarkHeartbeat(WorkerName);

                _logger.LogInformation(
                    "收到实时事件。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}",
                    evt.EventId,
                    evt.Type,
                    evt.TargetUserId);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                _readinessState.MarkHeartbeat(WorkerName);
                _logger.LogDebug("实时事件工作器空闲。P0 阶段尚未配置真实事件消费者。");
                await Task.Delay(_options.Value.WorkerIntervalMs, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("实时事件工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "实时事件工作器异常退出。");
            throw;
        }
        finally
        {
            _readinessState.MarkStopped(WorkerName);
            _logger.LogInformation("实时事件工作器已停止。");
        }
    }
}

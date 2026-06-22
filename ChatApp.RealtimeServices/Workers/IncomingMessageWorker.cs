using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Health;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class IncomingMessageWorker : BackgroundService
{
    private const string WorkerName = nameof(IncomingMessageWorker);

    private readonly IIncomingMessageConsumer _consumer;
    private readonly IIncomingMessageProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly IOptions<RealtimeOptions> _options;
    private readonly ILogger<IncomingMessageWorker> _logger;

    /// <summary>
    /// 负责处理入站消息的后台服务。该工作器持续监听并处理来自消息消费者的消息。
    /// </summary>
    /// <remarks>
    /// 本类继承自BackgroundService，实现了IHostedService接口，用于在应用程序启动时自动运行，并在应用程序停止时优雅地关闭。
    /// </remarks>
    public IncomingMessageWorker(
        IIncomingMessageConsumer consumer,
        IIncomingMessageProcessor processor,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        ILogger<IncomingMessageWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// 异步执行入站消息处理任务。此方法在后台服务启动时被调用，持续监听并处理来自消息消费者的消息，直到接收到停止信号。
    /// </summary>
    /// <param name="stoppingToken">用于通知此方法应准备优雅地停止的取消令牌。</param>
    /// <returns>返回一个表示异步操作的任务。</returns>
    /// <remarks>
    /// 该方法通过日志记录其生命周期的关键点，并使用<see cref="RealtimeReadinessState"/>来标记服务的运行状态（如启动、心跳和停止）。此外，它处理了不同类型的异常以确保服务能够正确关闭或报告故障。
    /// </remarks>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "入站消息工作器已启动。消费者实现={Consumer}；处理器实现={Processor}",
            _consumer.GetType().Name,
            _processor.GetType().Name);

        _readinessState.MarkStarted(WorkerName);

        try
        {
            await foreach (var command in _consumer.ConsumeAsync(stoppingToken).ConfigureAwait(false))
            {
                // 在处理每条消息之前标记心跳，以表明工作器正在正常运行。
                _readinessState.MarkHeartbeat(WorkerName);

                var result = await _processor
                    .ProcessAsync(command, stoppingToken)
                    .ConfigureAwait(false);

                if (result.Succeeded)
                {
                    _logger.LogInformation(
                        "入站消息处理成功。命令编号={CommandId}；消息编号={MessageId}",
                        command.CommandId,
                        result.MessageId ?? "<未生成>");
                }
                else
                {
                    _logger.LogWarning(
                        "入站消息处理失败。命令编号={CommandId}；错误码={ErrorCode}；错误信息={ErrorMessage}",
                        command.CommandId,
                        result.ErrorCode,
                        result.ErrorMessage);
                }
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                _readinessState.MarkHeartbeat(WorkerName);
                _logger.LogDebug("入站消息工作器空闲。P0 阶段尚未配置真实队列消费者。");
                await Task.Delay(_options.Value.WorkerIntervalMs, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("入站消息工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "入站消息工作器异常退出。");
            throw;
        }
        finally
        {
            _readinessState.MarkStopped(WorkerName);
            _logger.LogInformation("入站消息工作器已停止。");
        }
    }
}

using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class IncomingMessageWorker : BackgroundService
{
    private const string WorkerName = nameof(IncomingMessageWorker);
    private const ulong PoisonDeliveryThreshold = 8;

    private readonly IIncomingMessageConsumer _consumer;
    private readonly IIncomingMessageProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly IOptions<RealtimeOptions> _options;
    private readonly ILogger<IncomingMessageWorker> _logger;

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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "入站消息工作器已启动。消费者实现={Consumer}；处理器实现={Processor}",
            _consumer.GetType().Name,
            _processor.GetType().Name);

        _readinessState.MarkStarted(WorkerName);

        try
        {
            await foreach (var envelope in _consumer.ConsumeAsync(stoppingToken).ConfigureAwait(false))
            {
                _readinessState.MarkHeartbeat(WorkerName);

                if (IsPoison(envelope))
                {
                    _logger.LogCritical(
                        "检测到毒丸消息，直接丢弃。命令编号={CommandId}；投递次数={DeliveryCount}；阈值={Threshold}",
                        envelope.Command.CommandId,
                        envelope.DeliveryCount,
                        PoisonDeliveryThreshold);
                    await AckAsync(envelope, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var result = await _processor
                    .ProcessAsync(envelope.Command, stoppingToken)
                    .ConfigureAwait(false);

                if (result.Succeeded)
                {
                    await AckAsync(envelope, stoppingToken).ConfigureAwait(false);

                    _logger.LogInformation(
                        "入站消息处理成功。命令编号={CommandId}；消息编号={MessageId}",
                        envelope.Command.CommandId,
                        result.MessageId ?? "<未生成>");
                }
                else
                {
                    await NakAsync(envelope, stoppingToken).ConfigureAwait(false);

                    _logger.LogWarning(
                        "入站消息处理失败。命令编号={CommandId}；错误码={ErrorCode}；错误信息={ErrorMessage}",
                        envelope.Command.CommandId,
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

    private async ValueTask AckAsync(IncomingMessageEnvelope envelope, CancellationToken ct)
    {
        try
        {
            await envelope.AckAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "ACK 失败。命令编号={CommandId}",
                envelope.Command.CommandId);
        }
    }

    private async ValueTask NakAsync(IncomingMessageEnvelope envelope, CancellationToken ct)
    {
        try
        {
            await envelope.NakAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "NAK 失败。命令编号={CommandId}",
                envelope.Command.CommandId);
        }
    }

    private static bool IsPoison(IncomingMessageEnvelope envelope)
    {
        return envelope.DeliveryCount.HasValue
               && envelope.DeliveryCount.Value >= PoisonDeliveryThreshold;
    }
}

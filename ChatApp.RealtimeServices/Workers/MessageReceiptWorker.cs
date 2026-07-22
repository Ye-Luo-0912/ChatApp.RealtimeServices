using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class MessageReceiptWorker : BackgroundService
{
    private const string WorkerName = nameof(MessageReceiptWorker);
    private readonly IMessageReceiptConsumer _consumer;
    private readonly IMessageReceiptProcessor _processor;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueueOptions _queueOptions;
    private readonly ILogger<MessageReceiptWorker> _logger;

    public MessageReceiptWorker(
        IMessageReceiptConsumer consumer,
        IMessageReceiptProcessor processor,
        IDeadLetterPublisher deadLetterPublisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueueOptions queueOptions,
        ILogger<MessageReceiptWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _deadLetterPublisher = deadLetterPublisher;
        _readinessState = readinessState;
        _metrics = metrics;
        _options = options.Value;
        _queueOptions = queueOptions;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "消息回执工作器已启动。并发={Concurrency}；队列容量={Capacity}",
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity);
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);
        var channels = CreatePartitionChannels();
        var processors = channels
            .Select((channel, index) =>
                ProcessPartitionAsync(index, channel.Reader, stoppingToken))
            .ToArray();

        try
        {
            await ProduceAsync(channels, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("消息回执工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "消息回执生产循环异常退出。");
            throw;
        }
        finally
        {
            foreach (var channel in channels)
                channel.Writer.TryComplete();

            try
            {
                await Task.WhenAll(processors).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            finally
            {
                heartbeatCts.Cancel();
                await SuppressCancellationAsync(heartbeatTask).ConfigureAwait(false);
                _readinessState.MarkStopped(WorkerName);
            }
        }
    }

    private Channel<MessageReceiptEnvelope>[] CreatePartitionChannels()
    {
        var capacity = Math.Max(
            1,
            _options.ProcessingQueueCapacity / _options.ProcessingConcurrency);
        return Enumerable.Range(0, _options.ProcessingConcurrency)
            .Select(_ => Channel.CreateBounded<MessageReceiptEnvelope>(
                new BoundedChannelOptions(capacity)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait,
                    AllowSynchronousContinuations = false
                }))
            .ToArray();
    }

    private async Task ProduceAsync(
        IReadOnlyList<Channel<MessageReceiptEnvelope>> channels,
        CancellationToken ct)
    {
        var retryAttempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var envelope in _consumer
                                   .ConsumeAsync(ct)
                                   .ConfigureAwait(false))
                {
                    retryAttempt = 0;
                    _readinessState.MarkHeartbeat(WorkerName);
                    var partition = GetPartition(
                        envelope.Command.MessageId,
                        channels.Count);
                    await channels[partition].Writer
                        .WriteAsync(envelope, ct)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                retryAttempt++;
                var delay = TimeSpan.FromMilliseconds(
                    Math.Min(
                        30_000,
                        500 * Math.Pow(2, Math.Min(retryAttempt, 6))) +
                    Random.Shared.Next(0, 500));
                _logger.LogWarning(
                    ex,
                    "消息回执订阅中断，将重新订阅。延迟={Delay}",
                    delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            _readinessState.MarkHeartbeat(WorkerName);
            await Task.Delay(_options.WorkerIntervalMs, ct)
                .ConfigureAwait(false);
        }
    }

    private async Task ProcessPartitionAsync(
        int partition,
        ChannelReader<MessageReceiptEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader
                           .ReadAllAsync(ct)
                           .ConfigureAwait(false))
        {
            using var activity = RealtimeTelemetry.StartConsumer(
                "message_receipt.process",
                envelope.ParentContext);
            var started = Stopwatch.GetTimestamp();
            try
            {
                await ProcessOneAsync(envelope, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                RealtimeTelemetry.RecordException(activity, ex);
                _metrics.RecordProcessingFailure("receipt_unhandled");
                _logger.LogError(
                    ex,
                    "消息回执处理异常，将延迟重投。分区={Partition}；命令编号={CommandId}",
                    partition,
                    envelope.Command.CommandId);
                await TryNakAsync(
                    envelope,
                    TimeSpan.FromMilliseconds(_options.TransientRetryDelayMs),
                    ct).ConfigureAwait(false);
            }
            finally
            {
                _readinessState.MarkHeartbeat(WorkerName);
                _metrics.RecordProcessingDuration(
                    Stopwatch.GetElapsedTime(started));
            }
        }
    }

    private async Task ProcessOneAsync(
        MessageReceiptEnvelope envelope,
        CancellationToken ct)
    {
        if (envelope.DeliveryCount is not null &&
            envelope.DeliveryCount >=
            (ulong)_options.PoisonDeliveryThreshold)
        {
            await DeadLetterAndAckAsync(
                envelope,
                "max_receipt_deliveries",
                "消息回执投递次数达到毒丸阈值。",
                ct).ConfigureAwait(false);
            return;
        }

        var result = await _processor
            .ProcessAsync(envelope.Command, ct)
            .ConfigureAwait(false);
        if (result.Succeeded)
        {
            await TryAckAsync(envelope, ct).ConfigureAwait(false);
            return;
        }

        _metrics.RecordProcessingFailure(result.FailureKind.ToString());
        if (result.FailureKind == MessageFailureKind.Permanent)
        {
            await DeadLetterAndAckAsync(
                envelope,
                result.ErrorCode ?? "permanent_receipt_failure",
                result.ErrorMessage ?? "永久回执处理失败。",
                ct).ConfigureAwait(false);
            return;
        }

        await TryNakAsync(
            envelope,
            TimeSpan.FromMilliseconds(_options.TransientRetryDelayMs),
            ct).ConfigureAwait(false);
    }

    private async Task DeadLetterAndAckAsync(
        MessageReceiptEnvelope envelope,
        string reasonCode,
        string reason,
        CancellationToken ct)
    {
        var payload = envelope.RawPayload ?? JsonSerializer.Serialize(
            envelope.Command,
            RealtimeJsonSerializerContext.Default.MessageReceiptCommand);
        await _deadLetterPublisher.PublishAsync(
            new DeadLetterMessage
            {
                DeadLetterId = CreateDeadLetterId(
                    envelope.Command.CommandId,
                    reasonCode),
                CommandId = envelope.Command.CommandId,
                SourceSubject = _queueOptions.Topics.MessageReceipts,
                ReasonCode = reasonCode,
                Reason = reason,
                Payload = payload,
                DeliveryCount = envelope.DeliveryCount
            },
            ct).ConfigureAwait(false);
        _metrics.RecordDeadLetter(reasonCode);
        await TryAckAsync(envelope, ct).ConfigureAwait(false);
        _logger.LogWarning(
            "消息回执已进入死信流。命令编号={CommandId}；原因={ReasonCode}",
            envelope.Command.CommandId,
            reasonCode);
    }

    private async Task TryAckAsync(
        MessageReceiptEnvelope envelope,
        CancellationToken ct)
    {
        try
        {
            await envelope.AckAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("receipt_ack");
            _logger.LogError(
                ex,
                "消息回执 ACK 失败，可能被安全重投。命令编号={CommandId}",
                envelope.Command.CommandId);
        }
    }

    private async Task TryNakAsync(
        MessageReceiptEnvelope envelope,
        TimeSpan delay,
        CancellationToken ct)
    {
        try
        {
            await envelope.NakAsync(delay, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("receipt_nak");
            _logger.LogError(
                ex,
                "消息回执 NAK 失败，等待 AckWait 重投。命令编号={CommandId}",
                envelope.Command.CommandId);
        }
    }

    private async Task RunHeartbeatAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
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

    private static int GetPartition(
        string messageId,
        int partitionCount)
    {
        var hash = unchecked((uint)StringComparer.Ordinal.GetHashCode(messageId));
        return (int)(hash % (uint)partitionCount);
    }

    private static string CreateDeadLetterId(
        string commandId,
        string reasonCode)
    {
        var bytes = Encoding.UTF8.GetBytes(
            string.Concat(commandId, ":", reasonCode));
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }
}
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers.Reliability;
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
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly TimeSpan _ackWait;
    private readonly ILogger<MessageReceiptWorker> _logger;

    public MessageReceiptWorker(
        IMessageReceiptConsumer consumer,
        IMessageReceiptProcessor processor,
        IDeadLetterPublisher deadLetterPublisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueueOptions queueOptions,
        RealtimeNatsTrustSettings trust,
        JetStreamOptions? jetStreamOptions,
        ILogger<MessageReceiptWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _deadLetterPublisher = deadLetterPublisher;
        _readinessState = readinessState;
        _metrics = metrics;
        _options = options.Value;
        _trust = trust;
        _queueOptions = queueOptions;
        _ackWait = JetStreamAckTiming.GetEffectiveAckWait(jetStreamOptions);
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "消息回执工作器已启动。并发={Concurrency}；队列容量={Capacity}",
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity);
        var runtime = new PartitionedConsumerRuntime<MessageReceiptEnvelope>(
            WorkerName,
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity,
            _options.WorkerIntervalMs,
            _readinessState,
            _logger,
            ackWait: _ackWait,
            progressAckSelector: env => env.ProgressAckAsync);
        await runtime.RunAsync(
            consume: ct => _consumer.ConsumeAsync(ct),
            getPartition: env => GetPartition(env.Command.MessageId, _options.ProcessingConcurrency),
            processPartition: (partition, reader, ct) => ProcessPartitionAsync(partition, reader, ct),
            stoppingToken).ConfigureAwait(false);
    }

    private async Task ProcessPartitionAsync(
        int partition,
        ChannelReader<LeasedEnvelope<MessageReceiptEnvelope>> reader,
        CancellationToken ct)
    {
        await foreach (var leased in reader
                           .ReadAllAsync(ct)
                           .ConfigureAwait(false))
        {
            var envelope = leased.Envelope;
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
                // P0-6：lease 在处理完成时释放。MessageReceiptWorker 未启用字节预算，lease 始终为 null。
                if (leased.Lease is not null)
                    await leased.Lease.DisposeAsync().ConfigureAwait(false);
                // Reliability-4：ack lease 覆盖排队等待 + 处理全周期，处理完成时停止。
                // 轻量原子置位，快路径无异步清理、无异常。
                leased.AckLease.Complete();
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

        var identityError = NatsGatewayIdentity.ValidateReceiptReceiver(
            _trust.RequireGatewayIdentity,
            envelope.TrustedUserId,
            envelope.Command.ReceiverUserId);
        if (identityError is not null)
        {
            await DeadLetterAndAckAsync(
                envelope,
                identityError,
                "网关身份校验失败：payload 接收方与可信身份头不匹配或缺失。",
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
                DeadLetterId = DeadLetterIds.Create(
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

    private static int GetPartition(
        string messageId,
        int partitionCount)
    {
        var hash = unchecked((uint)StringComparer.Ordinal.GetHashCode(messageId));
        return (int)(hash % (uint)partitionCount);
    }
}

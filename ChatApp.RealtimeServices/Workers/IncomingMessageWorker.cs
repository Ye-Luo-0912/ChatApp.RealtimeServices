using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Messaging;
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

public sealed class IncomingMessageWorker : BackgroundService
{
    private const string WorkerName = nameof(IncomingMessageWorker);
    private readonly IIncomingMessageConsumer _consumer;
    private readonly IIncomingMessageProcessor _processor;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly IMessagePartitionKeySelector _partitionKeySelector;
    private readonly TimeSpan _ackWait;
    private readonly ILogger<IncomingMessageWorker> _logger;

    public IncomingMessageWorker(
        IIncomingMessageConsumer consumer,
        IIncomingMessageProcessor processor,
        IDeadLetterPublisher deadLetterPublisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeNatsTrustSettings trust,
        IMessagePartitionKeySelector partitionKeySelector,
        JetStreamOptions? jetStreamOptions,
        ILogger<IncomingMessageWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _deadLetterPublisher = deadLetterPublisher;
        _readinessState = readinessState;
        _metrics = metrics;
        _options = options.Value;
        _trust = trust;
        _partitionKeySelector = partitionKeySelector;
        _ackWait = jetStreamOptions is not null
            ? TimeSpan.FromSeconds(Math.Max(1, jetStreamOptions.Consumer.AckWaitSeconds))
            : TimeSpan.Zero;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "入站消息工作器已启动。消费者={Consumer}；处理器={Processor}；分区并发={Concurrency}；队列容量={Capacity}；字节预算={ByteBudget}",
            _consumer.GetType().Name,
            _processor.GetType().Name,
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity,
            _options.ProcessingQueueByteBudget);
        var runtime = new PartitionedConsumerRuntime<IncomingMessageEnvelope>(
            WorkerName,
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity,
            _options.WorkerIntervalMs,
            _readinessState,
            _logger,
            byteSizer: EnvelopeByteSizer,
            maxQueueBytes: _options.ProcessingQueueByteBudget,
            maxSinglePayloadBytes: _options.MaxSinglePayloadBytes);
        await runtime.RunAsync(
            consume: ct => _consumer.ConsumeAsync(ct),
            getPartition: env => GetPartition(env.Command, _options.ProcessingConcurrency),
            processPartition: (partition, reader, ct) => ProcessPartitionAsync(partition, reader, ct),
            stoppingToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Perf-6：按 RawPayload 的 UTF-8 字节长度计费。RawPayload 为 null 时回退到 Content 长度估算。
    /// </summary>
    private static long EnvelopeByteSizer(IncomingMessageEnvelope envelope)
    {
        if (envelope.RawPayload is { Length: > 0 } raw)
            return Encoding.UTF8.GetByteCount(raw);
        // 回退：Content 字符数 × 4（UTF-8 最坏情况）+ 结构开销
        return (envelope.Command.Content?.Length ?? 0) * 4L + 512;
    }

    private int GetPartition(IncomingMessageCommand command, int partitionCount)
    {
        // Perf-1：群聊按 ConversationId 分区，单聊按双方用户组合分区。
        var key = _partitionKeySelector.GetPartitionKey(command);
        return (int)(key % (ulong)partitionCount);
    }

    private async Task ProcessPartitionAsync(
        int partition,
        ChannelReader<LeasedEnvelope<IncomingMessageEnvelope>> reader,
        CancellationToken ct)
    {
        await foreach (var leased in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            var envelope = leased.Envelope;
            using var activity = RealtimeTelemetry.StartConsumer(
                "incoming_message.process",
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
                _metrics.RecordProcessingFailure("unhandled");
                _logger.LogError(
                    ex,
                    "入站消息处理出现未捕获异常，将延迟重投。分区={Partition}；命令编号={CommandId}",
                    partition,
                    envelope.Command.CommandId);
                await TryNakAsync(
                    envelope,
                    TimeSpan.FromMilliseconds(_options.TransientRetryDelayMs),
                    ct).ConfigureAwait(false);
            }
            finally
            {
                // P0-6：lease 在处理完成时释放，预算覆盖 queued + processing。
                // 不再在 Channel dequeue 时释放，确保正在执行的大消息始终计入内存预算。
                if (leased.Lease is not null)
                    await leased.Lease.DisposeAsync().ConfigureAwait(false);
                _readinessState.MarkHeartbeat(WorkerName);
                _metrics.RecordProcessingDuration(Stopwatch.GetElapsedTime(started));
            }
        }
    }

    private async Task ProcessOneAsync(IncomingMessageEnvelope envelope, CancellationToken ct)
    {
        // Perf-6：超大合法消息提前拒绝，避免单条消息占满字节预算。
        var payloadBytes = EnvelopeByteSizer(envelope);
        if (payloadBytes > _options.MaxSinglePayloadBytes)
        {
            await DeadLetterAndAckAsync(
                envelope,
                "payload_too_large",
                $"消息 payload 字节数 {payloadBytes} 超过单条上限 {_options.MaxSinglePayloadBytes}。",
                ct).ConfigureAwait(false);
            return;
        }

        if (envelope.DeliveryCount is not null
            && envelope.DeliveryCount >= (ulong)_options.PoisonDeliveryThreshold)
        {
            await DeadLetterAndAckAsync(
                envelope,
                "max_deliveries",
                "消息投递次数达到毒丸阈值。",
                ct).ConfigureAwait(false);
            return;
        }

        var identityError = NatsGatewayIdentity.ValidateIncomingSender(
            _trust.RequireGatewayIdentity,
            envelope.TrustedUserId,
            envelope.TrustedSessionId,
            envelope.Command.SenderUserId,
            envelope.Command.SenderSessionId);
        if (identityError is not null)
        {
            await DeadLetterAndAckAsync(
                envelope,
                identityError,
                "网关身份校验失败：payload 发送方与可信身份头不匹配或缺失。",
                ct).ConfigureAwait(false);
            return;
        }

        // Reliability-4：长时处理期间定期发送 In-Progress ACK，重置 JetStream AckWait 计时器。
        // 防止 队列等待 + 数据库处理 > AckWait 时合法消息被重投。
        await using var progressGuard = ProgressAckGuard.Start(
            envelope.ProgressAckAsync,
            _ackWait,
            ct,
            _logger);
        var result = await _processor.ProcessAsync(envelope.Command, ct).ConfigureAwait(false);
        if (result.Succeeded)
        {
            // Perf-6：成功解析后不长期保留完整 RawPayload，释放内存。
            envelope.ClearRawPayload();
            await TryAckAsync(envelope, ct).ConfigureAwait(false);
            _logger.LogDebug(
                "入站消息处理成功。命令编号={CommandId}；消息编号={MessageId}",
                envelope.Command.CommandId,
                result.MessageId);
            return;
        }

        _metrics.RecordProcessingFailure(result.FailureKind.ToString());
        if (result.FailureKind == MessageFailureKind.Permanent)
        {
            await DeadLetterAndAckAsync(
                envelope,
                result.ErrorCode ?? "permanent_failure",
                result.ErrorMessage ?? "永久处理失败。",
                ct).ConfigureAwait(false);
            return;
        }

        await TryNakAsync(
            envelope,
            TimeSpan.FromMilliseconds(_options.TransientRetryDelayMs),
            ct).ConfigureAwait(false);
    }

    private async Task DeadLetterAndAckAsync(
        IncomingMessageEnvelope envelope,
        string reasonCode,
        string reason,
        CancellationToken ct)
    {
        var payload = envelope.RawPayload ?? JsonSerializer.Serialize(
            envelope.Command,
            RealtimeJsonSerializerContext.Default.IncomingMessageCommand);
        await _deadLetterPublisher.PublishAsync(
            new DeadLetterMessage
            {
                DeadLetterId = DeadLetterIds.Create(envelope.Command.CommandId, reasonCode),
                CommandId = envelope.Command.CommandId,
                SourceSubject = "chat.incoming-messages",
                ReasonCode = reasonCode,
                Reason = reason,
                Payload = payload,
                DeliveryCount = envelope.DeliveryCount
            },
            ct).ConfigureAwait(false);
        _metrics.RecordDeadLetter(reasonCode);
        await TryAckAsync(envelope, ct).ConfigureAwait(false);
        _logger.LogWarning(
            "入站消息已进入死信流。命令编号={CommandId}；原因={ReasonCode}；投递次数={DeliveryCount}",
            envelope.Command.CommandId,
            reasonCode,
            envelope.DeliveryCount);
    }

    private async Task TryAckAsync(IncomingMessageEnvelope envelope, CancellationToken ct)
    {
        try
        {
            await envelope.AckAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("ack");
            _logger.LogError(ex, "ACK 失败，消息可能被安全重投。命令编号={CommandId}", envelope.Command.CommandId);
        }
    }

    private async Task TryNakAsync(
        IncomingMessageEnvelope envelope,
        TimeSpan delay,
        CancellationToken ct)
    {
        try
        {
            await envelope.NakAsync(delay, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("nak");
            _logger.LogError(ex, "NAK 失败，等待 AckWait 触发重投。命令编号={CommandId}", envelope.Command.CommandId);
        }
    }
}

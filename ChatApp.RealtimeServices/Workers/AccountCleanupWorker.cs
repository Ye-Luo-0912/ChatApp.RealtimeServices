using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers.Reliability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 消费 Realtime 域事件并执行账号删除清理（非网关推送路径）。
/// Reliability-2：迁入 <see cref="PartitionedConsumerRuntime{TEnvelope}"/>，
/// 获得订阅重连退避、SubscriptionConnected 信号、RecordMessageConsumed 进展上报、
/// processor fault propagation 与 QueueDepth 报告。枚举结束后自动重新订阅而非进入永久 heartbeat。
/// </summary>
public sealed class AccountCleanupWorker : BackgroundService
{
    private const string WorkerName = nameof(AccountCleanupWorker);

    private readonly IRealtimeEventConsumer _consumer;
    private readonly IUserAccountDeletedProcessor _processor;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueueOptions _queueOptions;
    private readonly ILogger<AccountCleanupWorker> _logger;

    public AccountCleanupWorker(
        IRealtimeEventConsumer consumer,
        IUserAccountDeletedProcessor processor,
        IDeadLetterPublisher deadLetterPublisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueueOptions queueOptions,
        ILogger<AccountCleanupWorker> logger)
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
            "账号清理工作器已启动。消费者={Consumer}；处理器={Processor}；分区并发={Concurrency}；队列容量={Capacity}",
            _consumer.GetType().Name,
            _processor.GetType().Name,
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity);

        var runtime = new PartitionedConsumerRuntime<RealtimeEventEnvelope>(
            WorkerName,
            _options.ProcessingConcurrency,
            _options.ProcessingQueueCapacity,
            _options.WorkerIntervalMs,
            _readinessState,
            _logger);

        await runtime.RunAsync(
            consume: ct => _consumer.ConsumeAsync(ct),
            getPartition: env => GetPartition(env.Event.TargetUserId, _options.ProcessingConcurrency),
            processPartition: (partition, reader, ct) => ProcessPartitionAsync(partition, reader, ct),
            stoppingToken).ConfigureAwait(false);
    }

    private async Task ProcessPartitionAsync(
        int partition,
        ChannelReader<LeasedEnvelope<RealtimeEventEnvelope>> reader,
        CancellationToken ct)
    {
        await foreach (var leased in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            var envelope = leased.Envelope;
            using var activity = RealtimeTelemetry.StartConsumer(
                "account_cleanup.process",
                default);
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
                _metrics.RecordProcessingFailure("cleanup_unhandled");
                _logger.LogError(
                    ex,
                    "账号清理处理异常，将 NAK 等待重投。分区={Partition}；事件={EventId}",
                    partition,
                    envelope.Event.EventId);
                await TryNakAsync(envelope, ct).ConfigureAwait(false);
            }
            finally
            {
                if (leased.Lease is not null)
                    await leased.Lease.DisposeAsync().ConfigureAwait(false);
                _readinessState.MarkHeartbeat(WorkerName);
                _metrics.RecordProcessingDuration(Stopwatch.GetElapsedTime(started));
            }
        }
    }

    private async Task ProcessOneAsync(RealtimeEventEnvelope envelope, CancellationToken ct)
    {
        var evt = envelope.Event;

        // 毒丸阈值：投递次数达到上限，转入 DLQ 并 ACK，避免无限重投。
        if (envelope.DeliveryCount is not null
            && envelope.DeliveryCount >= (ulong)_options.PoisonDeliveryThreshold)
        {
            await DeadLetterAndAckAsync(
                envelope,
                "max_cleanup_deliveries",
                "账号清理事件投递次数达到毒丸阈值。",
                ct).ConfigureAwait(false);
            return;
        }

        // 账号清理 worker 仅处理 UserAccountDeleted；其它事件类型直接 ACK 跳过。
        if (evt.Type is RealtimeEventType.AccountCleanupCompleted
            or RealtimeEventType.AttachmentBlobsPurge)
        {
            // 完成 / blob GC 事件由其它订阅方（Server）处理；清理 worker 直接 ACK。
            await TryAckAsync(envelope, ct).ConfigureAwait(false);
            return;
        }

        if (evt.Type != RealtimeEventType.UserAccountDeleted)
        {
            // 同 subject 上的其它事件：ACK 跳过，不阻塞清理队列。
            await TryAckAsync(envelope, ct).ConfigureAwait(false);
            return;
        }

        var result = await _processor.ProcessAsync(evt, ct).ConfigureAwait(false);
        if (result.Succeeded)
        {
            await TryAckAsync(envelope, ct).ConfigureAwait(false);
            _logger.LogDebug(
                "账号清理成功。事件={EventId}；目标用户={TargetUserId}",
                evt.EventId,
                evt.TargetUserId);
            return;
        }

        _metrics.RecordProcessingFailure(result.FailureKind.ToString());
        _logger.LogWarning(
            "账号清理失败。事件={EventId}；错误={ErrorCode}；投递次数={DeliveryCount}",
            evt.EventId,
            result.ErrorCode,
            envelope.DeliveryCount);

        if (result.FailureKind == MessageFailureKind.Permanent)
        {
            // 永久失败：转入 DLQ 并 ACK，避免毒丸阻塞队列。
            await DeadLetterAndAckAsync(
                envelope,
                result.ErrorCode ?? "cleanup_permanent",
                result.ErrorMessage ?? "账号清理永久失败。",
                ct).ConfigureAwait(false);
            return;
        }

        // 瞬时失败：NAK 等待重投（JetStream durable Backoff 控制重投间隔）。
        await TryNakAsync(envelope, ct).ConfigureAwait(false);
    }

    private async Task DeadLetterAndAckAsync(
        RealtimeEventEnvelope envelope,
        string reasonCode,
        string reason,
        CancellationToken ct)
    {
        var evt = envelope.Event;
        var payload = JsonSerializer.Serialize(
            evt,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);
        await _deadLetterPublisher.PublishAsync(
            new DeadLetterMessage
            {
                DeadLetterId = DeadLetterIds.Create(evt.EventId, reasonCode),
                CommandId = evt.EventId,
                SourceSubject = _queueOptions.Topics.AccountCleanup,
                ReasonCode = reasonCode,
                Reason = reason,
                Payload = payload,
                DeliveryCount = envelope.DeliveryCount
            },
            ct).ConfigureAwait(false);
        _metrics.RecordDeadLetter(reasonCode);
        await TryAckAsync(envelope, ct).ConfigureAwait(false);
        _logger.LogWarning(
            "账号清理事件已进入死信流。事件={EventId}；原因={ReasonCode}；投递次数={DeliveryCount}",
            evt.EventId,
            reasonCode,
            envelope.DeliveryCount);
    }

    private async Task TryAckAsync(RealtimeEventEnvelope envelope, CancellationToken ct)
    {
        try
        {
            await envelope.AckAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("cleanup_ack");
            _logger.LogError(
                ex,
                "账号清理 ACK 失败，消息可能被安全重投。事件={EventId}",
                envelope.Event.EventId);
        }
    }

    private async Task TryNakAsync(RealtimeEventEnvelope envelope, CancellationToken ct)
    {
        try
        {
            await envelope.NakAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordProcessingFailure("cleanup_nak");
            _logger.LogError(
                ex,
                "账号清理 NAK 失败，等待 AckWait 触发重投。事件={EventId}",
                envelope.Event.EventId);
        }
    }

    private static int GetPartition(long targetUserId, int partitionCount)
    {
        // 按 TargetUserId 分区，确保同一用户的清理事件串行处理。
        var key = unchecked((ulong)targetUserId);
        return (int)(key % (ulong)partitionCount);
    }
}

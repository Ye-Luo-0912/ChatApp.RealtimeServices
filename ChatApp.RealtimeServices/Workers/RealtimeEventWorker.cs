using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers.Reliability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 实时事件 Worker：消费 NATS realtime-events 主题。
/// <para>
/// LongTerm-2：<see cref="AccountCleanupWorker"/> 已迁出 NATS 消费转为 Saga 轮询，
/// 本 Worker 接管 <see cref="RealtimeEventType.UserAccountDeleted"/> 事件：
/// 调用 <see cref="IUserAccountDeletedProcessor"/> 写入 tombstone + 入队清理作业后立即 ACK。
/// 其它事件类型（网关推送路径已停用）直接 ACK 跳过。
/// </para>
/// <para>
/// 可靠性由 JetStream durable consumer 的 AckWait + MaxDeliver 保证：
/// Transient 失败 NAK 触发重投；Permanent 失败（含毒丸，由 Processor 判定后返回
/// <see cref="MessageFailureKind.Permanent"/>）转入 DLQ 并 ACK。
/// 消费枚举意外结束后自动重新订阅，而非进入空转心跳。
/// </para>
/// </summary>
public sealed class RealtimeEventWorker : BackgroundService
{
    private const string WorkerName = nameof(RealtimeEventWorker);

    private readonly IRealtimeEventConsumer _consumer;
    private readonly IUserAccountDeletedProcessor _accountDeletedProcessor;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueueOptions _queueOptions;
    private readonly ILogger<RealtimeEventWorker> _logger;

    public RealtimeEventWorker(
        IRealtimeEventConsumer consumer,
        IUserAccountDeletedProcessor accountDeletedProcessor,
        IDeadLetterPublisher deadLetterPublisher,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        RealtimeQueueOptions queueOptions,
        ILogger<RealtimeEventWorker> logger)
    {
        _consumer = consumer;
        _accountDeletedProcessor = accountDeletedProcessor;
        _deadLetterPublisher = deadLetterPublisher;
        _readinessState = readinessState;
        _options = options.Value;
        _queueOptions = queueOptions;
        _logger = logger;
    }

    /// <summary>
    /// 执行异步任务，启动实时事件处理流程。
    /// 该方法在后台服务启动时被调用，并持续运行直到接收到停止信号。
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "实时事件 Worker 已启动。消费者={Consumer}；处理器={Processor}",
            _consumer.GetType().Name,
            _accountDeletedProcessor.GetType().Name);

        _readinessState.MarkStarted(WorkerName);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var envelope in _consumer.ConsumeAsync(stoppingToken).ConfigureAwait(false))
                    {
                        _readinessState.MarkHeartbeat(WorkerName);
                        var evt = envelope.Event;

                        try
                        {
                            if (evt.Type == RealtimeEventType.UserAccountDeleted)
                            {
                                // LongTerm-2：调用处理器写入 tombstone + 入队清理作业，立即返回。
                                // 重型清理由 AccountCleanupWorker Saga 按 phase 分批推进。
                                var result = await _accountDeletedProcessor
                                    .ProcessAsync(evt, stoppingToken)
                                    .ConfigureAwait(false);

                                if (!result.Succeeded)
                                {
                                    if (result.FailureKind == MessageFailureKind.Permanent)
                                    {
                                        _logger.LogError(
                                            "账号删除处理永久失败，转入 DLQ。事件={EventId}；错误={ErrorCode}",
                                            evt.EventId,
                                            result.ErrorCode);
                                        await DeadLetterAndAckAsync(
                                            envelope,
                                            result.ErrorCode ?? "permanent_event_failure",
                                            result.ErrorMessage ?? "实时事件永久处理失败。",
                                            stoppingToken).ConfigureAwait(false);
                                        continue;
                                    }

                                    _logger.LogWarning(
                                        "账号删除处理失败，将 NAK 等待重投。事件={EventId}；错误={ErrorCode}",
                                        evt.EventId,
                                        result.ErrorCode);
                                    await TryNakAsync(envelope, stoppingToken).ConfigureAwait(false);
                                    continue;
                                }
                            }

                            // 其它事件类型（网关推送路径已停用）与处理成功的事件：直接 ACK 跳过。
                            await envelope.AckAsync(stoppingToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(
                                ex,
                                "实时事件处理异常，将 NAK 等待重投。事件={EventId}",
                                evt.EventId);
                            await TryNakAsync(envelope, stoppingToken).ConfigureAwait(false);
                        }
                    }

                    // 枚举正常结束（不应发生）：记录警告，下方延迟后重新订阅。
                    _logger.LogWarning("实时事件消费枚举意外结束，将在短暂延迟后重新订阅。");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "实时事件消费循环异常，将在延迟后重新订阅。");
                }

                if (!stoppingToken.IsCancellationRequested)
                {
                    _readinessState.MarkHeartbeat(WorkerName);
                    try
                    {
                        await Task.Delay(_options.WorkerIntervalMs, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("实时事件 Worker 正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "实时事件 Worker 异常退出。");
            throw;
        }
        finally
        {
            _readinessState.MarkStopped(WorkerName);
            _logger.LogInformation("实时事件 Worker 已停止。");
        }
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
        await TryAckAsync(envelope, ct).ConfigureAwait(false);
        _logger.LogWarning(
            "实时事件已进入死信流。事件={EventId}；原因={ReasonCode}；投递次数={DeliveryCount}",
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
            _logger.LogError(
                ex,
                "实时事件 ACK 失败，可能被安全重投。事件={EventId}",
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
            _logger.LogError(
                ex,
                "实时事件 NAK 失败，等待 AckWait 触发重投。事件={EventId}",
                envelope.Event.EventId);
        }
    }
}

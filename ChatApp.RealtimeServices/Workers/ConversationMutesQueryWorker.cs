using System.Diagnostics;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.RealtimeServices.Concurrency;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 会话免打扰批量查询工作器（ACCOUNT-OPS-1）：Gateway 离线推送触发前经
/// Core NATS request/reply 查询"当前生效免打扰"的成员集合。只读路径，
/// 复用读池并发预算与过载协议（enqueue/gate 满时回 ServerBusy）。
/// </summary>
public sealed class ConversationMutesQueryWorker : BackgroundService
{
    private const string WorkerName = nameof(ConversationMutesQueryWorker);
    private const string QueueKind = "conversation_mutes";
    private const QueryPoolKind PoolKind = QueryPoolKind.Read;
    private readonly IConversationMutesQueryConsumer _consumer;
    private readonly IConversationMutesQueryProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueryConcurrencyGate _queryGate;
    private readonly ILogger<ConversationMutesQueryWorker> _logger;

    public ConversationMutesQueryWorker(
        IConversationMutesQueryConsumer consumer,
        IConversationMutesQueryProcessor processor,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueryConcurrencyGate queryGate,
        ILogger<ConversationMutesQueryWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _metrics = metrics;
        _options = options.Value;
        _queryGate = queryGate;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "会话免打扰查询工作器已启动。共享并发={Concurrency}；队列容量={Capacity}；工作槽={Slots}",
            _queryGate.ReadPermits,
            _options.HistoryQueryQueueCapacity,
            Math.Max(1, _options.HistoryQueryWorkerSlots));
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);
        var channel = Channel.CreateBounded<ConversationMutesQueryEnvelope>(
            new BoundedChannelOptions(_options.HistoryQueryQueueCapacity)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            });
        var processors = Enumerable
            .Range(0, Math.Max(1, _options.HistoryQueryWorkerSlots))
            .Select(index => ProcessAsync(index, channel.Reader, stoppingToken))
            .ToArray();

        try
        {
            await ProduceAsync(channel.Writer, stoppingToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("会话免打扰查询工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "会话免打扰查询订阅循环异常退出。");
            throw;
        }
        finally
        {
            channel.Writer.TryComplete();
            try
            {
                await Task.WhenAll(processors).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }

            heartbeatCts.Cancel();
            await SuppressCancellationAsync(heartbeatTask).ConfigureAwait(false);
            _readinessState.MarkStopped(WorkerName);
        }
    }

    private async Task ProduceAsync(
        ChannelWriter<ConversationMutesQueryEnvelope> writer,
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
                    _metrics.HistoryQueryEnqueued();
                    if (_options.OverloadEnqueueTimeoutMs > 0)
                    {
                        using var enqueueTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        enqueueTimeout.CancelAfter(TimeSpan.FromMilliseconds(_options.OverloadEnqueueTimeoutMs));
                        try
                        {
                            await writer.WriteAsync(envelope, enqueueTimeout.Token)
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                        {
                            _metrics.RecordOverloadReply(QueueKind, "enqueue");
                            await envelope.ReplyAsync(
                                ConversationMutesQueryResult.ServerBusy(
                                    envelope.Query.RequestId,
                                    _options.OverloadRetryAfterMs,
                                    QueueKind),
                                ct).ConfigureAwait(false);
                            continue;
                        }
                    }
                    else
                    {
                        try
                        {
                            await writer.WriteAsync(envelope, ct)
                                .ConfigureAwait(false);
                        }
                        catch
                        {
                            _metrics.HistoryQueryEnqueueFailed();
                            throw;
                        }
                    }
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
                    Math.Min(30_000, 500 * Math.Pow(2, Math.Min(retryAttempt, 6)))
                    + Random.Shared.Next(0, 500));
                _logger.LogWarning(
                    ex,
                    "会话免打扰查询订阅中断，将重新订阅。延迟={Delay}",
                    delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }

            _readinessState.MarkHeartbeat(WorkerName);
            await Task.Delay(_options.WorkerIntervalMs, ct)
                .ConfigureAwait(false);
        }
    }

    private async Task ProcessAsync(
        int workerIndex,
        ChannelReader<ConversationMutesQueryEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            _metrics.HistoryQueryStarted();
            using var activity = RealtimeTelemetry.StartConsumer(
                "conversation_mutes_query.process",
                envelope.ParentContext);
            var started = Stopwatch.GetTimestamp();
            var succeeded = false;
            string? outcome = "cancelled";
            try
            {
                var gateAcquired = await _queryGate
                    .WaitAsync(PoolKind, _options.OverloadGateTimeoutMs, ct)
                    .ConfigureAwait(false);
                if (!gateAcquired)
                {
                    _metrics.RecordOverloadReply(QueueKind, "gate");
                    outcome = "server_busy";
                    try
                    {
                        await envelope.ReplyAsync(
                            ConversationMutesQueryResult.ServerBusy(
                                envelope.Query.RequestId,
                                _options.OverloadRetryAfterMs,
                                QueueKind),
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        outcome = "reply_failed";
                        _logger.LogWarning(
                            ex,
                            "会话免打扰查询过载响应发送失败。请求编号={RequestId}",
                            envelope.Query.RequestId);
                    }
                    continue;
                }
                ConversationMutesQueryResult result;
                try
                {
                    // ACCOUNT-OPS-1：Gateway → Realtime 服务间查询，无单用户身份语义
                    //（同一查询覆盖多个成员），不做 per-user 身份头校验；
                    // 信任边界为 NATS 账户认证 + subject ACL。
                    result = await _processor
                        .ProcessAsync(envelope.Query, ct)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    RealtimeTelemetry.RecordException(activity, ex);
                    _logger.LogError(
                        ex,
                        "会话免打扰查询失败。工作槽={WorkerIndex}；会话={ConversationId}；请求编号={RequestId}",
                        workerIndex,
                        envelope.Query.ConversationId,
                        envelope.Query.RequestId);
                    result = ConversationMutesQueryResult.Failed(
                        envelope.Query.RequestId,
                        "conversation_mutes_query_unavailable",
                        _options.EnableDetailedErrors
                            ? ex.Message
                            : "会话免打扰查询服务暂时不可用，请稍后重试。");
                }
                finally
                {
                    _queryGate.Release(PoolKind);
                }

                // Reply 在 permit 释放后执行，避免 NATS 网络时间占用数据库 permit
                try
                {
                    await envelope.ReplyAsync(result, ct).ConfigureAwait(false);
                    succeeded = result.Succeeded;
                    outcome = result.ErrorCode;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    outcome = "reply_failed";
                    _logger.LogWarning(
                        ex,
                        "会话免打扰查询响应发送失败。请求编号={RequestId}",
                        envelope.Query.RequestId);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            finally
            {
                _metrics.RecordHistoryQuery(
                    succeeded,
                    outcome,
                    Stopwatch.GetElapsedTime(started));
                _readinessState.MarkHeartbeat(WorkerName);
            }
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
}

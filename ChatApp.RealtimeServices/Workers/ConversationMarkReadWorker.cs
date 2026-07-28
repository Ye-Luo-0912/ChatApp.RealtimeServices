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

public sealed class ConversationMarkReadWorker : BackgroundService
{
    private const string WorkerName = nameof(ConversationMarkReadWorker);
    private const QueryPoolKind PoolKind = QueryPoolKind.Interactive;
    private readonly IConversationMarkReadConsumer _consumer;
    private readonly IConversationMarkReadProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueryConcurrencyGate _queryGate;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly ILogger<ConversationMarkReadWorker> _logger;

    public ConversationMarkReadWorker(
        IConversationMarkReadConsumer consumer,
        IConversationMarkReadProcessor processor,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueryConcurrencyGate queryGate,
        RealtimeNatsTrustSettings trust,
        ILogger<ConversationMarkReadWorker> logger)
    {
        _consumer = consumer;
        _processor = processor;
        _readinessState = readinessState;
        _metrics = metrics;
        _options = options.Value;
        _queryGate = queryGate;
        _trust = trust;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "会话已读标记工作器已启动。共享并发={Concurrency}；队列容量={Capacity}；工作槽={Slots}",
            _queryGate.InteractivePermits,
            _options.HistoryQueryQueueCapacity,
            Math.Max(1, _options.HistoryQueryWorkerSlots));
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);
        var channel = Channel.CreateBounded<ConversationMarkReadEnvelope>(
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
            _logger.LogInformation("会话已读标记工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "会话已读标记订阅循环异常退出。");
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
        ChannelWriter<ConversationMarkReadEnvelope> writer,
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
                            _metrics.RecordOverloadReply("conversation_mark_read", "enqueue");
                            await envelope.ReplyAsync(
                                ConversationMarkReadResult.ServerBusy(
                                    envelope.Command.RequestId,
                                    _options.OverloadRetryAfterMs,
                                    "conversation_mark_read"),
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
                    "会话已读标记订阅中断，将重新订阅。延迟={Delay}",
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
        ChannelReader<ConversationMarkReadEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            _metrics.HistoryQueryStarted();
            using var activity = RealtimeTelemetry.StartConsumer(
                "conversation_mark_read.process",
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
                    _metrics.RecordOverloadReply("conversation_mark_read", "gate");
                    outcome = "server_busy";
                    try
                    {
                        await envelope.ReplyAsync(
                            ConversationMarkReadResult.ServerBusy(
                                envelope.Command.RequestId,
                                _options.OverloadRetryAfterMs,
                                "conversation_mark_read"),
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        outcome = "reply_failed";
                        _logger.LogWarning(
                            ex,
                            "会话已读标记过载响应发送失败。请求编号={RequestId}",
                            envelope.Command.RequestId);
                    }
                    continue;
                }
                ConversationMarkReadResult result;
                try
                {
                    var identityError = NatsGatewayIdentity.ValidateHistoryUser(
                        _trust.RequireGatewayIdentity,
                        envelope.TrustedUserId,
                        envelope.Command.UserId);
                    if (identityError is not null)
                    {
                        result = ConversationMarkReadResult.Failed(
                            envelope.Command.RequestId,
                            identityError,
                            "网关身份校验失败：会话已读用户与可信身份头不匹配或缺失。");
                    }
                    else
                    {
                        result = await _processor
                            .ProcessAsync(envelope.Command, ct)
                            .ConfigureAwait(false);
                    }
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
                        "会话已读标记失败。工作槽={WorkerIndex}；请求编号={RequestId}",
                        workerIndex,
                        envelope.Command.RequestId);
                    result = ConversationMarkReadResult.Failed(
                        envelope.Command.RequestId,
                        "conversation_mark_read_unavailable",
                        _options.EnableDetailedErrors
                            ? ex.Message
                            : "会话已读服务暂时不可用，请稍后重试。");
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
                        "会话已读标记响应发送失败。请求编号={RequestId}",
                        envelope.Command.RequestId);
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

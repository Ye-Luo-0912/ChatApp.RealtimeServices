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

public sealed class ConversationListQueryWorker : BackgroundService
{
    private const string WorkerName = nameof(ConversationListQueryWorker);
    private const QueryPoolKind PoolKind = QueryPoolKind.Read;
    private readonly IConversationListQueryConsumer _consumer;
    private readonly IConversationListQueryProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueryConcurrencyGate _queryGate;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly ILogger<ConversationListQueryWorker> _logger;

    public ConversationListQueryWorker(
        IConversationListQueryConsumer consumer,
        IConversationListQueryProcessor processor,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueryConcurrencyGate queryGate,
        RealtimeNatsTrustSettings trust,
        ILogger<ConversationListQueryWorker> logger)
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
            "会话列表查询工作器已启动。共享并发={Concurrency}；队列容量={Capacity}；工作槽={Slots}",
            _queryGate.ReadPermits,
            _options.HistoryQueryQueueCapacity,
            Math.Max(1, _options.HistoryQueryWorkerSlots));
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);
        var channel = Channel.CreateBounded<ConversationListQueryEnvelope>(
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
            _logger.LogInformation("会话列表查询工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "会话列表查询订阅循环异常退出。");
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
        ChannelWriter<ConversationListQueryEnvelope> writer,
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
                            _metrics.RecordOverloadReply("conversation_list", "enqueue");
                            await envelope.ReplyAsync(
                                ConversationListPage.ServerBusy(
                                    envelope.Query.RequestId,
                                    _options.OverloadRetryAfterMs,
                                    "conversation_list"),
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
                    "会话列表查询订阅中断，将重新订阅。延迟={Delay}",
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
        ChannelReader<ConversationListQueryEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            _metrics.HistoryQueryStarted();
            using var activity = RealtimeTelemetry.StartConsumer(
                "conversation_list_query.process",
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
                    _metrics.RecordOverloadReply("conversation_list", "gate");
                    outcome = "server_busy";
                    try
                    {
                        await envelope.ReplyAsync(
                            ConversationListPage.ServerBusy(
                                envelope.Query.RequestId,
                                _options.OverloadRetryAfterMs,
                                "conversation_list"),
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        outcome = "reply_failed";
                        _logger.LogWarning(
                            ex,
                            "会话列表查询过载响应发送失败。请求编号={RequestId}",
                            envelope.Query.RequestId);
                    }
                    continue;
                }
                ConversationListPage page;
                try
                {
                    var identityError = NatsGatewayIdentity.ValidateHistoryUser(
                        _trust.RequireGatewayIdentity,
                        envelope.TrustedUserId,
                        envelope.Query.UserId);
                    if (identityError is not null)
                    {
                        page = ConversationListPage.Failed(
                            envelope.Query.RequestId,
                            identityError,
                            "网关身份校验失败：会话列表用户与可信身份头不匹配或缺失。");
                    }
                    else
                    {
                        page = await _processor
                            .ProcessAsync(envelope.Query, ct)
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
                        "会话列表查询失败。工作槽={WorkerIndex}；请求编号={RequestId}",
                        workerIndex,
                        envelope.Query.RequestId);
                    page = ConversationListPage.Failed(
                        envelope.Query.RequestId,
                        "conversation_list_unavailable",
                        _options.EnableDetailedErrors
                            ? ex.Message
                            : "会话列表服务暂时不可用，请稍后重试。");
                }
                finally
                {
                    _queryGate.Release(PoolKind);
                }

                // Reply 在 permit 释放后执行，避免 NATS 网络时间占用数据库 permit
                try
                {
                    await envelope.ReplyAsync(page, ct).ConfigureAwait(false);
                    succeeded = page.Succeeded;
                    outcome = page.ErrorCode;
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
                        "会话列表查询响应发送失败。请求编号={RequestId}",
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

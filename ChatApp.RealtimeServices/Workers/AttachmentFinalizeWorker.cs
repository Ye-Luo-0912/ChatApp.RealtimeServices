using System.Diagnostics;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Attachments;
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

public sealed class AttachmentFinalizeWorker : BackgroundService
{
    private const string WorkerName = nameof(AttachmentFinalizeWorker);
    private const QueryPoolKind PoolKind = QueryPoolKind.Mutation;
    private readonly IAttachmentFinalizeConsumer _consumer;
    private readonly IAttachmentFinalizeProcessor _processor;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _options;
    private readonly RealtimeQueryConcurrencyGate _queryGate;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly ILogger<AttachmentFinalizeWorker> _logger;

    public AttachmentFinalizeWorker(
        IAttachmentFinalizeConsumer consumer,
        IAttachmentFinalizeProcessor processor,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> options,
        RealtimeQueryConcurrencyGate queryGate,
        RealtimeNatsTrustSettings trust,
        ILogger<AttachmentFinalizeWorker> logger)
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
            "附件确认工作器已启动。变更并发={Concurrency}；队列容量={Capacity}；处理槽={Slots}",
            _queryGate.MutationPermits,
            _options.HistoryQueryQueueCapacity,
            Math.Max(1, _options.HistoryQueryWorkerSlots));
        _readinessState.MarkStarted(WorkerName);

        using var heartbeatCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeatTask = RunHeartbeatAsync(heartbeatCts.Token);
        var channel = Channel.CreateBounded<AttachmentFinalizeEnvelope>(
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
            _logger.LogInformation("附件确认工作器正在停止。");
        }
        catch (Exception ex)
        {
            _readinessState.MarkFaulted(WorkerName, ex);
            _logger.LogError(ex, "附件确认工作器发生异常。");
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
        ChannelWriter<AttachmentFinalizeEnvelope> writer,
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
                            _metrics.RecordOverloadReply("attachment_finalize", "enqueue");
                            await envelope.ReplyAsync(
                                AttachmentFinalizeResult.ServerBusy(
                                    envelope.Command.RequestId,
                                    _options.OverloadRetryAfterMs,
                                    "attachment_finalize"),
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
                    "附件确认消费循环异常，将在 {Delay} 后重试。",
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
        ChannelReader<AttachmentFinalizeEnvelope> reader,
        CancellationToken ct)
    {
        await foreach (var envelope in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            _metrics.HistoryQueryStarted();
            using var activity = RealtimeTelemetry.StartConsumer(
                "attachment_finalize.process",
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
                    _metrics.RecordOverloadReply("attachment_finalize", "gate");
                    outcome = "server_busy";
                    try
                    {
                        await envelope.ReplyAsync(
                            AttachmentFinalizeResult.ServerBusy(
                                envelope.Command.RequestId,
                                _options.OverloadRetryAfterMs,
                                "attachment_finalize"),
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        outcome = "reply_failed";
                        _logger.LogWarning(
                            ex,
                            "附件确认过载响应发送失败。请求编号={RequestId}",
                            envelope.Command.RequestId);
                    }
                    continue;
                }
                AttachmentFinalizeResult result;
                try
                {
                    var identityError = NatsGatewayIdentity.ValidateHistoryUser(
                        _trust.RequireGatewayIdentity,
                        envelope.TrustedUserId,
                        envelope.Command.ActorUserId);
                    if (identityError is not null)
                    {
                        result = AttachmentFinalizeResult.Failed(
                            envelope.Command.RequestId,
                            identityError,
                            "网关身份头校验失败，拒绝处理附件确认命令。");
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
                        "附件确认处理异常。工作器={WorkerIndex}；请求编号={RequestId}",
                        workerIndex,
                        envelope.Command.RequestId);
                    result = AttachmentFinalizeResult.Failed(
                        envelope.Command.RequestId,
                        "attachment_finalize_unavailable",
                        _options.EnableDetailedErrors
                            ? ex.Message
                            : "附件上传确认服务暂时不可用。");
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
                        "附件确认响应发送失败。请求编号={RequestId}",
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

using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class OutboxPublisherWorker : BackgroundService
{
    private const string WorkerName = nameof(OutboxPublisherWorker);
    private readonly IRealtimeOutboxStore _outboxStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly IRealtimeEventPublisher _publisher;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeMetrics _metrics;
    private readonly RealtimeOptions _realtimeOptions;
    private readonly OutboxOptions _options;
    private readonly TimeSpan _pollInterval;
    private readonly ILogger<OutboxPublisherWorker> _logger;

    public OutboxPublisherWorker(
        IRealtimeOutboxStore outboxStore,
        IRealtimeOutboxSignal outboxSignal,
        IRealtimeEventPublisher publisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> realtimeOptions,
        IOptions<OutboxOptions> options,
        ILogger<OutboxPublisherWorker> logger)
    {
        _outboxStore = outboxStore;
        _outboxSignal = outboxSignal;
        _publisher = publisher;
        _readinessState = readinessState;
        _metrics = metrics;
        _realtimeOptions = realtimeOptions.Value;
        _options = options.Value;
        _pollInterval = TimeSpan.FromMilliseconds(_options.PollIntervalMs);
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _readinessState.MarkStarted(WorkerName);
        var retryAttempt = 0;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _readinessState.MarkHeartbeat(WorkerName);
                    var records = await _outboxStore.ClaimBatchAsync(
                        _realtimeOptions.InstanceId,
                        _options.BatchSize,
                        TimeSpan.FromSeconds(_options.LeaseSeconds),
                        stoppingToken).ConfigureAwait(false);

                    if (records.Count == 0)
                    {
                        retryAttempt = 0;
                        await _outboxSignal
                            .WaitAsync(_pollInterval, stoppingToken)
                            .ConfigureAwait(false);
                        continue;
                    }

                    await Parallel.ForEachAsync(
                        records,
                        new ParallelOptions
                        {
                            MaxDegreeOfParallelism = _options.PublishConcurrency,
                            CancellationToken = stoppingToken
                        },
                        PublishOneAsync).ConfigureAwait(false);
                    retryAttempt = 0;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    retryAttempt++;
                    _readinessState.MarkFaulted(WorkerName, ex);
                    var delay = CalculateWorkerRetryDelay(retryAttempt);
                    _logger.LogWarning(
                        ex,
                        "Outbox 存储或发布循环暂时失败，将继续重试。尝试次数={AttemptCount}；延迟={Delay}",
                        retryAttempt,
                        delay);
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Outbox 发布工作器正在停止。");
        }
        finally
        {
            _readinessState.MarkStopped(WorkerName);
        }
    }

    private static TimeSpan CalculateWorkerRetryDelay(int attemptCount)
    {
        var milliseconds = Math.Min(
            30_000,
            500 * Math.Pow(2, Math.Min(attemptCount - 1, 6)));
        return TimeSpan.FromMilliseconds(milliseconds + Random.Shared.Next(0, 500));
    }

    private async ValueTask PublishOneAsync(RealtimeOutboxRecord record, CancellationToken ct)
    {
        var parentContext = RealtimeTraceContext.Parse(
            record.Event.TraceParent,
            record.Event.TraceState);
        using var activity = RealtimeTelemetry.StartOutboxPublish(parentContext);
        activity?.SetTag("chat.event.type", record.Event.Type.ToString());
        try
        {
            if (record.Event.TargetUserIds is { Length: > 0 })
            {
                await _publisher.PublishToManyAsync(record.Event, ct).ConfigureAwait(false);
            }
            else
            {
                await _publisher.PublishAsync(record.Event, ct).ConfigureAwait(false);
            }
            await _outboxStore.MarkPublishedAsync(record, ct).ConfigureAwait(false);
            _metrics.RecordOutboxPublished();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            RealtimeTelemetry.RecordException(activity, ex);
            _metrics.RecordOutboxFailure();
            if (record.AttemptCount >= _options.MaxAttempts)
            {
                await _outboxStore
                    .MarkDeadAsync(record, ex.Message, ct)
                    .ConfigureAwait(false);
                _metrics.RecordOutboxDeadLetter();
                _logger.LogError(
                    ex,
                    "Outbox 事件已进入死信。事件编号={EventId}；尝试次数={AttemptCount}",
                    record.EventId,
                    record.AttemptCount);
                return;
            }

            var delay = CalculateRetryDelay(record.AttemptCount);
            await _outboxStore.MarkFailedAsync(record, ex.Message, delay, ct).ConfigureAwait(false);
            _logger.LogWarning(
                ex,
                "Outbox 事件发布失败，将重试。事件编号={EventId}；尝试次数={AttemptCount}；延迟={Delay}",
                record.EventId,
                record.AttemptCount,
                delay);
        }
    }

    private TimeSpan CalculateRetryDelay(int attemptCount)
    {
        var seconds = Math.Min(
            _options.MaxRetryDelaySeconds,
            Math.Pow(2, Math.Min(attemptCount, 10)));
        return TimeSpan.FromMilliseconds(seconds * 1000 + Random.Shared.Next(0, 500));
    }
}

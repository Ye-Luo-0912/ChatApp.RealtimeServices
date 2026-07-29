using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
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

                    // P1-3：记录认领时间，发布前据此判断是否需要续租 lease。
                    var claimedAt = DateTimeOffset.UtcNow;
                    await PublishBatchAsync(records, claimedAt, stoppingToken).ConfigureAwait(false);
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

    /// <summary>
    /// P1-3/P0-9：并行发布一批记录，收集结果后按状态分组批量更新，避免逐事件数据库往返。
    /// 发布与状态更新解耦：发布失败但 lease 未过期的记录会在下一轮被原实例或其它实例重新认领。
    /// <para>
    /// P0-9：启动独立 renew loop 在发布期间周期性续租 lease（每 lease/3 检查一次），
    /// 避免长批次发布超过 lease 后其他实例重复认领。续租部分失败时，丢失所有权的记录
    /// 不标记 Published，让其他实例重新处理。
    /// </para>
    /// </summary>
    private async Task PublishBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        DateTimeOffset claimedAt,
        CancellationToken ct)
    {
        var lease = TimeSpan.FromSeconds(_options.LeaseSeconds);
        var renewInterval = TimeSpan.FromTicks(lease.Ticks / 3);

        // P0-9：启动独立续租 loop，在发布期间周期性续租 lease。
        var lostOwnershipEventIds = new HashSet<string>(StringComparer.Ordinal);
        using var renewCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewTask = Task.Run(async () =>
        {
            while (!renewCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(renewInterval, renewCts.Token).ConfigureAwait(false);
                    var renewed = await _outboxStore
                        .ExtendLeaseBatchAsync(records, lease, renewCts.Token)
                        .ConfigureAwait(false);
                    if (renewed < records.Count)
                    {
                        _logger.LogWarning(
                            "Outbox lease 续租部分失败：{Renewed}/{Total}，可能丢失部分记录所有权。",
                            renewed,
                            records.Count);
                        // 续租数少于总数：无法确定哪些记录丢失所有权，
                        // 保守地将所有记录标记为丢失，避免误 MarkPublished 他人认领的记录。
                        // 已成功发布的记录依赖 JetStream MsgId 去重，重试时不产生重复投递。
                        lock (lostOwnershipEventIds)
                        {
                            foreach (var r in records)
                                lostOwnershipEventIds.Add(r.EventId);
                        }
                    }
                }
                catch (OperationCanceledException) when (renewCts.Token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Outbox lease 续租失败，继续尝试。");
                }
            }
        }, renewCts.Token);

        try
        {
        var published = new List<RealtimeOutboxRecord>(records.Count);
        var failed = new List<(RealtimeOutboxRecord Record, string Error, TimeSpan Delay)>(records.Count);
        var deadLetters = new List<(RealtimeOutboxRecord Record, string Error)>(records.Count);

        var results = new PublishOutcome[records.Count];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, records.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.PublishConcurrency,
                CancellationToken = ct
            },
            async (index, token) =>
            {
                var record = records[index];

                // P0-9：检查记录是否丢失所有权（续租失败），跳过发布。
                bool lostOwnership;
                lock (lostOwnershipEventIds)
                {
                    lostOwnership = lostOwnershipEventIds.Contains(record.EventId);
                }
                if (lostOwnership)
                {
                    results[index] = new PublishOutcome(record, Succeeded: false, Error: "lease_lost");
                    return;
                }

                // 四-1：路由信息来自数据库列（唯一权威），不从反序列化 payload 读取。
                // record.Event 为 null 时（新记录），构造最小 RealtimeEvent 供 Publisher 接口使用。
                var routingEvent = record.Event ?? new RealtimeEvent
                {
                    EventId = record.EventId,
                    Type = record.EventType,
                    TargetUserId = record.TargetUserId,
                    TargetUserIds = record.TargetUserIds,
                    AudienceKind = record.AudienceKind,
                    ConversationId = record.ConversationId,
                    OccurredAtMs = 0,
                    TraceParent = record.TraceParent,
                    TraceState = record.TraceState,
                };
                var parentContext = RealtimeTraceContext.Parse(
                    record.TraceParent,
                    record.TraceState);
                using var activity = RealtimeTelemetry.StartOutboxPublish(parentContext);
                activity?.SetTag("chat.event.type", record.EventType.ToString());
                try
                {
                    // 四-1：路由判断使用列字段。会话级受众（Conversation）也走多目标路径。
                    var isMultiTarget = record.TargetUserIds is { Length: > 0 }
                        || record.AudienceKind == AudienceKind.Conversation;
                    // 五：优先直接发送预序列化的 UTF-8 字节，避免重新序列化。
                    var payload = record.PayloadUtf8;
                    if (isMultiTarget)
                    {
                        if (payload is { Length: > 0 })
                        {
                            await _publisher.PublishToManyWithPayloadAsync(routingEvent, payload, token).ConfigureAwait(false);
                        }
                        else
                        {
                            await _publisher.PublishToManyAsync(routingEvent, token).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        if (payload is { Length: > 0 })
                        {
                            await _publisher.PublishWithPayloadAsync(routingEvent, payload, token).ConfigureAwait(false);
                        }
                        else
                        {
                            await _publisher.PublishAsync(routingEvent, token).ConfigureAwait(false);
                        }
                    }
                    results[index] = new PublishOutcome(record, Succeeded: true, Error: null);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    RealtimeTelemetry.RecordException(activity, ex);
                    _metrics.RecordOutboxFailure();
                    results[index] = new PublishOutcome(record, Succeeded: false, Error: ex.Message);
                }
            }).ConfigureAwait(false);

        // 按状态分组
        foreach (var outcome in results)
        {
            if (outcome.Succeeded)
            {
                published.Add(outcome.Record);
            }
            else if (outcome.Record.AttemptCount >= _options.MaxAttempts)
            {
                deadLetters.Add((outcome.Record, outcome.Error!));
                _metrics.RecordOutboxDeadLetter();
            }
            else
            {
                var delay = CalculateRetryDelay(outcome.Record.AttemptCount);
                failed.Add((outcome.Record, outcome.Error!, delay));
            }
        }

        // 批量状态更新：用实际命中记录数更新 metrics，避免 lease 过期被其他实例接手后仍计数。
        if (published.Count > 0)
        {
            var publishedAffected = await _outboxStore
                .MarkPublishedBatchAsync(published, ct)
                .ConfigureAwait(false);
            for (var i = 0; i < publishedAffected; i++)
                _metrics.RecordOutboxPublished();
        }

        if (failed.Count > 0)
        {
            await _outboxStore.MarkFailedBatchAsync(failed, ct).ConfigureAwait(false);
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                foreach (var (record, error, delay) in failed)
                {
                    _logger.LogWarning(
                        "Outbox 事件发布失败，将重试。事件编号={EventId}；尝试次数={AttemptCount}；延迟={Delay}; 错误={Error}",
                        record.EventId,
                        record.AttemptCount,
                        delay,
                        error);
                }
            }
        }

        if (deadLetters.Count > 0)
        {
            await _outboxStore.MarkDeadBatchAsync(deadLetters, ct).ConfigureAwait(false);
            if (_logger.IsEnabled(LogLevel.Error))
            {
                foreach (var (record, error) in deadLetters)
                {
                    _logger.LogError(
                        "Outbox 事件已进入死信。事件编号={EventId}；尝试次数={AttemptCount}; 错误={Error}",
                        record.EventId,
                        record.AttemptCount,
                        error);
                }
            }
        }
        }
        finally
        {
            // P0-9：停止续租 loop 并等待退出。
            renewCts.Cancel();
            try
            {
                await renewTask.ConfigureAwait(false);
            }
            catch
            {
                // 续租 task 异常已在内层 catch 处理，此处忽略。
            }
        }
    }

    private TimeSpan CalculateRetryDelay(int attemptCount)
    {
        var seconds = Math.Min(
            _options.MaxRetryDelaySeconds,
            Math.Pow(2, Math.Min(attemptCount, 10)));
        return TimeSpan.FromMilliseconds(seconds * 1000 + Random.Shared.Next(0, 500));
    }

    private readonly record struct PublishOutcome(
        RealtimeOutboxRecord Record,
        bool Succeeded,
        string? Error);
}

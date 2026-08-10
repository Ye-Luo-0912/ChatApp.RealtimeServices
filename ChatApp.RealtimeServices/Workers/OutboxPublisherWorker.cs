using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Concurrency;
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
    private readonly IRealtimeOutboxHintSource? _hintSource;
    private readonly IRealtimeOutboxHintClaimStore? _hintClaimStore;
    private readonly IRealtimeOutboxPreclaimedStore? _preclaimedStore;
    private readonly IRealtimeOutboxPreclaimCoordinator? _preclaimCoordinator;
    private readonly IRealtimeOutboxClaimSessionFactory? _claimSessionFactory;
    private readonly IRelationshipProjectionStore? _relationshipProjectionStore;
    private readonly List<string> _hintBatch;
    private readonly List<string> _preclaimedEventIds;
    private readonly List<string> _preclaimedTokens;
    private readonly List<RealtimeOutboxRecord> _claimedBatch;
    private readonly List<RealtimeOutboxRecord> _pendingPublished;
    private long _nextCompletionFlushAt;
    private readonly ILogger<OutboxPublisherWorker> _logger;

    public OutboxPublisherWorker(
        IRealtimeOutboxStore outboxStore,
        IRealtimeOutboxSignal outboxSignal,
        IRealtimeEventPublisher publisher,
        RealtimeReadinessState readinessState,
        RealtimeMetrics metrics,
        IOptions<RealtimeOptions> realtimeOptions,
        IOptions<OutboxOptions> options,
        ILogger<OutboxPublisherWorker> logger,
        IRelationshipProjectionStore? relationshipProjectionStore = null)
    {
        _outboxStore = outboxStore;
        _outboxSignal = outboxSignal;
        _publisher = publisher;
        _readinessState = readinessState;
        _metrics = metrics;
        _realtimeOptions = realtimeOptions.Value;
        _options = options.Value;
        _hintSource = outboxSignal as IRealtimeOutboxHintSource;
        _hintClaimStore = outboxStore as IRealtimeOutboxHintClaimStore;
        _preclaimedStore = outboxStore as IRealtimeOutboxPreclaimedStore;
        _preclaimCoordinator = outboxSignal as IRealtimeOutboxPreclaimCoordinator;
        _claimSessionFactory = outboxStore as IRealtimeOutboxClaimSessionFactory;
        _relationshipProjectionStore = relationshipProjectionStore;
        _hintBatch = new List<string>(_options.BatchSize);
        _preclaimedEventIds = new List<string>(_options.BatchSize);
        _preclaimedTokens = new List<string>(_options.BatchSize);
        _claimedBatch = new List<RealtimeOutboxRecord>(_options.BatchSize);
        _pendingPublished = new List<RealtimeOutboxRecord>(_options.CompletionBatchSize);
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _readinessState.MarkStarted(WorkerName);
        var retryAttempt = 0;
        var lease = TimeSpan.FromSeconds(_options.LeaseSeconds);
        if (_preclaimedStore is not null)
            _preclaimCoordinator?.ConfigurePreclaimOwner(_realtimeOptions.InstanceId, lease);
        var recoveryIntervalMs = _hintSource is not null && _hintClaimStore is not null
            ? _options.RecoveryScanIntervalMs
            : _options.PollIntervalMs;
        var nextRecoveryScanAt = 0L;
        IRealtimeOutboxClaimSession? claimSession = null;
        await using var leaseScheduler = new SharedLeaseScheduler<OutboxLeaseState>(
            TimeSpan.FromTicks(lease.Ticks / 3),
            RenewOutboxLeaseAsync,
            ex => _logger.LogWarning(ex, "Outbox lease 续租失败，继续尝试。"));
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _readinessState.MarkHeartbeat(WorkerName);
                    await FlushPublishedAsync(force: false, stoppingToken).ConfigureAwait(false);
                    if (claimSession is null && _claimSessionFactory is not null)
                    {
                        claimSession = await _claimSessionFactory
                            .OpenClaimSessionAsync(_realtimeOptions.InstanceId, stoppingToken)
                            .ConfigureAwait(false);
                    }
                    IReadOnlyList<RealtimeOutboxRecord> records;
                    var now = Environment.TickCount64;
                    if (now >= nextRecoveryScanAt)
                    {
                        records = claimSession is null
                            ? await _outboxStore.ClaimBatchAsync(
                                _realtimeOptions.InstanceId,
                                _options.BatchSize,
                                lease,
                                stoppingToken).ConfigureAwait(false)
                            : await claimSession.ClaimBatchAsync(
                                _options.BatchSize,
                                lease,
                                stoppingToken).ConfigureAwait(false);
                        _metrics.RecordOutboxRecoveryScan();
                        nextRecoveryScanAt = Environment.TickCount64
                                             + Math.Max(1, recoveryIntervalMs);
                    }
                    else if (TryDrainCommittedHints())
                    {
                        await CoalesceCommittedHintsAsync(
                            nextRecoveryScanAt,
                            stoppingToken).ConfigureAwait(false);
                        _claimedBatch.Clear();
                        if (_preclaimedEventIds.Count > 0)
                        {
                            var preclaimed = claimSession is null
                                ? await _preclaimedStore!.ReadPreclaimedAsync(
                                    _realtimeOptions.InstanceId,
                                    _preclaimedEventIds,
                                    _preclaimedTokens,
                                    _options.BatchSize,
                                    stoppingToken).ConfigureAwait(false)
                                : await claimSession.ReadPreclaimedAsync(
                                    _preclaimedEventIds,
                                    _preclaimedTokens,
                                    _options.BatchSize,
                                    stoppingToken).ConfigureAwait(false);
                            _claimedBatch.AddRange(preclaimed);
                            _metrics.RecordOutboxHintClaim(
                                _preclaimedEventIds.Count,
                                preclaimed.Count);
                        }

                        if (_hintBatch.Count > 0)
                        {
                            var claimed = claimSession is null
                                ? await _hintClaimStore!.ClaimBatchByIdsAsync(
                                    _realtimeOptions.InstanceId,
                                    _hintBatch,
                                    _options.BatchSize,
                                    lease,
                                    stoppingToken).ConfigureAwait(false)
                                : await claimSession.ClaimBatchByIdsAsync(
                                    _hintBatch,
                                    _options.BatchSize,
                                    lease,
                                    stoppingToken).ConfigureAwait(false);
                            _claimedBatch.AddRange(claimed);
                            _metrics.RecordOutboxHintClaim(_hintBatch.Count, claimed.Count);
                        }

                        records = _claimedBatch;
                    }
                    else
                    {
                        // 有界提示实现存在且恢复周期尚未到期：不做空 Pending 扫描。
                        records = [];
                    }

                    if (records.Count == 0)
                    {
                        retryAttempt = 0;
                        await _outboxSignal
                            .WaitAsync(GetIdleWaitInterval(nextRecoveryScanAt), stoppingToken)
                            .ConfigureAwait(false);
                        continue;
                    }

                    await PublishBatchAsync(records, leaseScheduler, stoppingToken).ConfigureAwait(false);
                    retryAttempt = 0;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    retryAttempt++;
                    if (claimSession is not null)
                    {
                        await DisposeClaimSessionAsync(claimSession).ConfigureAwait(false);
                        claimSession = null;
                    }
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
            await TryFlushPublishedOnShutdownAsync().ConfigureAwait(false);
            if (claimSession is not null)
                await DisposeClaimSessionAsync(claimSession).ConfigureAwait(false);
            _readinessState.MarkStopped(WorkerName);
        }
    }

    private bool TryDrainCommittedHints()
    {
        _hintBatch.Clear();
        _preclaimedEventIds.Clear();
        _preclaimedTokens.Clear();
        if (_hintSource is null || _hintClaimStore is null)
            return false;

        DrainCommittedHints();
        return _hintBatch.Count > 0 || _preclaimedEventIds.Count > 0;
    }

    private void DrainCommittedHints()
    {
        while (_hintBatch.Count + _preclaimedEventIds.Count < _options.BatchSize
               && _hintSource!.TryReadCommittedHint(out var hint))
        {
            if (_preclaimedStore is not null
                && hint.IsPreclaimed
                && string.Equals(
                    hint.LockOwner,
                    _realtimeOptions.InstanceId,
                    StringComparison.Ordinal))
            {
                _preclaimedEventIds.Add(hint.EventId);
                _preclaimedTokens.Add(hint.ClaimToken!);
            }
            else
            {
                _hintBatch.Add(hint.EventId);
            }
        }
    }

    private async ValueTask CoalesceCommittedHintsAsync(
        long nextRecoveryScanAt,
        CancellationToken ct)
    {
        var delayMs = CalculateHintCoalescingDelayMilliseconds(
            _options.HintCoalescingWindowMs,
            _hintBatch.Count + _preclaimedEventIds.Count,
            _options.BatchSize,
            nextRecoveryScanAt,
            _pendingPublished.Count == 0 ? 0 : _nextCompletionFlushAt,
            Environment.TickCount64);
        if (delayMs <= 0)
            return;

        // 单个极短、有界 delay 换取更大的预领取读取批次；窗口不会滚动延长，
        // 且上面的期限判断保证不推迟 recovery 或发布完成刷新。
        await Task.Delay(delayMs, ct).ConfigureAwait(false);
        DrainCommittedHints();
    }

    internal static int CalculateHintCoalescingDelayMilliseconds(
        int configuredWindowMs,
        int bufferedHintCount,
        int batchSize,
        long nextRecoveryScanAt,
        long nextCompletionFlushAt,
        long now)
    {
        if (configuredWindowMs <= 0 || bufferedHintCount <= 0 || bufferedHintCount >= batchSize)
            return 0;

        // 不为合批跨过可靠性扫描或已确认发布的完成刷新期限。
        if (nextRecoveryScanAt - now <= configuredWindowMs)
            return 0;
        if (nextCompletionFlushAt > 0
            && nextCompletionFlushAt - now <= configuredWindowMs)
        {
            return 0;
        }

        return configuredWindowMs;
    }

    private async ValueTask DisposeClaimSessionAsync(IRealtimeOutboxClaimSession session)
    {
        try
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "释放 Outbox 独占认领会话失败。");
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
        SharedLeaseScheduler<OutboxLeaseState> leaseScheduler,
        CancellationToken ct)
    {
        if (records.Count == 0)
            return;

        var lease = TimeSpan.FromSeconds(_options.LeaseSeconds);
        var registration = leaseScheduler.Register(new OutboxLeaseState(records, lease), ct);
        try
        {
            // 正式 8h 运行平均只有约 1.13 条/批。单条快路径避免 Parallel.ForEachAsync、
            // 结果数组和多个临时 List，仍保留 claim_token 所有权校验。
            if (records.Count == 1)
            {
                await PublishSingleAsync(records[0], registration, ct).ConfigureAwait(false);
                return;
            }

            await PublishManyAsync(records, registration, ct).ConfigureAwait(false);
        }
        finally
        {
            registration.Complete();
        }
    }

    private async Task PublishSingleAsync(
        RealtimeOutboxRecord record,
        SharedLeaseRegistration<OutboxLeaseState> registration,
        CancellationToken ct)
    {
        if (registration.RenewalRejected)
            return;

        var outcome = await PublishRecordAsync(record, ct).ConfigureAwait(false);
        if (registration.RenewalRejected)
            return;

        if (outcome.Succeeded)
        {
            QueuePublished(record);
            return;
        }

        var error = outcome.Error ?? "publish_failed";
        if (record.AttemptCount >= _options.MaxAttempts)
        {
            var affected = await _outboxStore
                .MarkDeadBatchAsync([(record, error)], ct)
                .ConfigureAwait(false);
            for (var i = 0; i < affected; i++)
                _metrics.RecordOutboxDeadLetter();
            LogDeadLetter(record, error);
            return;
        }

        var delay = CalculateRetryDelay(record.AttemptCount);
        await _outboxStore
            .MarkFailedAsync(record, error, delay, ct)
            .ConfigureAwait(false);
        LogPublishFailure(record, error, delay);
    }

    private async Task PublishManyAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        SharedLeaseRegistration<OutboxLeaseState> registration,
        CancellationToken ct)
    {
        var results = new PublishOutcome[records.Count];
        await Parallel.ForAsync(
            0,
            records.Count,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.PublishConcurrency,
                CancellationToken = ct
            },
            async (index, token) =>
            {
                var record = records[index];
                results[index] = registration.RenewalRejected
                    ? new PublishOutcome(record, Succeeded: false, Error: "lease_lost")
                    : await PublishRecordAsync(record, token).ConfigureAwait(false);
            }).ConfigureAwait(false);

        // 续租部分失败时无法知道具体哪些记录仍归本实例所有。整个批次不再写完成状态，
        // 让租约到期后的新所有者安全重试；JetStream MsgId 负责已发布事件的幂等去重。
        if (registration.RenewalRejected)
            return;

        List<(RealtimeOutboxRecord Record, string Error, TimeSpan Delay)>? failed = null;
        List<(RealtimeOutboxRecord Record, string Error)>? deadLetters = null;
        for (var i = 0; i < results.Length; i++)
        {
            var outcome = results[i];
            if (outcome.Succeeded)
            {
                QueuePublished(outcome.Record);
            }
            else if (outcome.Record.AttemptCount >= _options.MaxAttempts)
            {
                (deadLetters ??= new List<(RealtimeOutboxRecord, string)>()).Add(
                    (outcome.Record, outcome.Error ?? "publish_failed"));
            }
            else
            {
                var delay = CalculateRetryDelay(outcome.Record.AttemptCount);
                (failed ??= new List<(RealtimeOutboxRecord, string, TimeSpan)>()).Add(
                    (outcome.Record, outcome.Error ?? "publish_failed", delay));
            }
        }

        if (failed is not null)
        {
            await _outboxStore.MarkFailedBatchAsync(failed, ct).ConfigureAwait(false);
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                for (var i = 0; i < failed.Count; i++)
                {
                    var item = failed[i];
                    LogPublishFailure(item.Record, item.Error, item.Delay);
                }
            }
        }

        if (deadLetters is not null)
        {
            var affected = await _outboxStore
                .MarkDeadBatchAsync(deadLetters, ct)
                .ConfigureAwait(false);
            for (var i = 0; i < affected; i++)
                _metrics.RecordOutboxDeadLetter();
            if (_logger.IsEnabled(LogLevel.Error))
            {
                for (var i = 0; i < deadLetters.Count; i++)
                    LogDeadLetter(deadLetters[i].Record, deadLetters[i].Error);
            }
        }
    }

    private Task<int> CompletePublishedAsync(
        RealtimeOutboxRecord record,
        CancellationToken ct)
    {
        if (_options.PublishedRetentionHours <= 0
            && _outboxStore is IRealtimeOutboxCompactionStore compactionStore)
        {
            return compactionStore.DeleteClaimedPublishedAsync(record, ct);
        }

        return _outboxStore.TryMarkPublishedAsync(record, ct);
    }

    private Task<int> CompletePublishedBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        CancellationToken ct)
    {
        if (_options.PublishedRetentionHours <= 0
            && _outboxStore is IRealtimeOutboxCompactionStore compactionStore)
        {
            return compactionStore.DeleteClaimedPublishedBatchAsync(records, ct);
        }

        return _outboxStore.MarkPublishedBatchAsync(records, ct);
    }

    private void QueuePublished(RealtimeOutboxRecord record)
    {
        if (_pendingPublished.Count == 0)
        {
            _nextCompletionFlushAt = Environment.TickCount64
                                     + Math.Max(1, _options.CompletionFlushIntervalMs);
        }

        _pendingPublished.Add(record);
    }

    private async Task FlushPublishedAsync(bool force, CancellationToken ct)
    {
        if (_pendingPublished.Count == 0)
            return;
        if (!force
            && _pendingPublished.Count < _options.CompletionBatchSize
            && Environment.TickCount64 < _nextCompletionFlushAt)
        {
            return;
        }

        var affected = _pendingPublished.Count == 1
            ? await CompletePublishedAsync(_pendingPublished[0], ct).ConfigureAwait(false)
            : await CompletePublishedBatchAsync(_pendingPublished, ct).ConfigureAwait(false);
        for (var i = 0; i < affected; i++)
            _metrics.RecordOutboxPublished();

        // claim_token 使未命中的行只能是租约已丢失/已完成；清空本地引用，让恢复扫描接管。
        _pendingPublished.Clear();
        _nextCompletionFlushAt = 0;
    }

    private TimeSpan GetIdleWaitInterval(long nextRecoveryScanAt)
    {
        var now = Environment.TickCount64;
        var recoveryRemainingMs = Math.Max(0, nextRecoveryScanAt - now);
        if (_pendingPublished.Count == 0)
            return TimeSpan.FromMilliseconds(recoveryRemainingMs);

        var completionRemainingMs = Math.Max(0, _nextCompletionFlushAt - now);
        var remainingMs = Math.Min(recoveryRemainingMs, completionRemainingMs);
        if (remainingMs == 0)
            return TimeSpan.Zero;
        return TimeSpan.FromMilliseconds(remainingMs);
    }

    private async Task TryFlushPublishedOnShutdownAsync()
    {
        if (_pendingPublished.Count == 0)
            return;

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await FlushPublishedAsync(force: true, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // NATS 已确认但数据库尚未完成的记录会在 lease 到期后以同 EventId 重试。
            _logger.LogWarning(
                ex,
                "停止时刷新 Outbox 完成批次失败，将由租约恢复。待处理={PendingCount}",
                _pendingPublished.Count);
        }
    }

    private async ValueTask<PublishOutcome> PublishRecordAsync(
        RealtimeOutboxRecord record,
        CancellationToken ct)
    {
        var routingEvent = record.Event ?? new RealtimeEvent
        {
            EventId = record.EventId,
            Type = record.EventType,
            TargetUserId = record.TargetUserId,
            TargetUserIds = record.TargetUserIds,
            AudienceKind = record.AudienceKind,
            ConversationId = record.ConversationId,
            ExcludeUserId = record.ExcludeUserId,
            OccurredAtMs = 0,
            TraceParent = record.TraceParent,
            TraceState = record.TraceState,
        };
        var parentContext = RealtimeTraceContext.Parse(record.TraceParent, record.TraceState);
        using var activity = RealtimeTelemetry.StartOutboxPublish(parentContext);
        activity?.SetTag("chat.event.type", record.EventType.ToString());
        try
        {
            await RelationshipProjectionPrePublisher
                .ApplyAsync(record, _relationshipProjectionStore, ct)
                .ConfigureAwait(false);
            var isMultiTarget = record.TargetUserIds is { Length: > 0 }
                || record.AudienceKind == AudienceKind.Conversation;
            var payload = record.PayloadUtf8;
            if (isMultiTarget)
            {
                if (payload is { Length: > 0 })
                    await _publisher.PublishToManyWithPayloadAsync(routingEvent, payload, ct).ConfigureAwait(false);
                else
                    await _publisher.PublishToManyAsync(routingEvent, ct).ConfigureAwait(false);
            }
            else if (payload is { Length: > 0 })
            {
                await _publisher.PublishWithPayloadAsync(routingEvent, payload, ct).ConfigureAwait(false);
            }
            else
            {
                await _publisher.PublishAsync(routingEvent, ct).ConfigureAwait(false);
            }

            return new PublishOutcome(record, Succeeded: true, Error: null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            RealtimeTelemetry.RecordException(activity, ex);
            _metrics.RecordOutboxFailure();
            return new PublishOutcome(record, Succeeded: false, Error: ex.Message);
        }
    }

    private async ValueTask<bool> RenewOutboxLeaseAsync(
        OutboxLeaseState state,
        CancellationToken schedulerStoppingToken)
    {
        var renewed = await _outboxStore
            .ExtendLeaseBatchAsync(state.Records, state.Lease, schedulerStoppingToken)
            .ConfigureAwait(false);
        if (renewed >= state.Records.Count)
            return true;

        _logger.LogWarning(
            "Outbox lease 续租部分失败：{Renewed}/{Total}，本批次将停止完成状态写入。",
            renewed,
            state.Records.Count);
        return false;
    }

    private void LogPublishFailure(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan delay) =>
        _logger.LogWarning(
            "Outbox 事件发布失败，将重试。事件编号={EventId}；尝试次数={AttemptCount}；延迟={Delay}; 错误={Error}",
            record.EventId,
            record.AttemptCount,
            delay,
            error);

    private void LogDeadLetter(RealtimeOutboxRecord record, string error) =>
        _logger.LogError(
            "Outbox 事件已进入死信。事件编号={EventId}；尝试次数={AttemptCount}; 错误={Error}",
            record.EventId,
            record.AttemptCount,
            error);

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

    internal readonly record struct OutboxLeaseState(
        IReadOnlyList<RealtimeOutboxRecord> Records,
        TimeSpan Lease);
}

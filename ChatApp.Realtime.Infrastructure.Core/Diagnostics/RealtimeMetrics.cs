using System.Diagnostics.Metrics;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Diagnostics;

public sealed class RealtimeMetrics : IDisposable
{
    public const string MeterName = "ChatApp.RealtimeServices";

    private readonly Meter _meter = new(MeterName, "1.0.0");
    private readonly Counter<long> _persistedCounter;
    private readonly Counter<long> _duplicateCounter;
    private readonly Counter<long> _receiptAppliedCounter;
    private readonly Counter<long> _receiptDuplicateCounter;
    private readonly Counter<long> _failureCounter;
    private readonly Counter<long> _deadLetterCounter;
    private readonly Counter<long> _outboxPublishedCounter;
    private readonly Counter<long> _outboxFailureCounter;
    private readonly Counter<long> _historyQueryCounter;
    private readonly Counter<long> _historyQueryFailureCounter;
    private readonly ObservableGauge<long> _historyQueryQueueDepthGauge;
    private readonly ObservableGauge<long> _historyQueriesInFlightGauge;
    private readonly ObservableGauge<long> _outboxPendingGauge;
    private readonly ObservableGauge<double> _outboxOldestAgeGauge;
    private readonly ObservableGauge<double> _outboxOldestInFlightAgeGauge;
    private readonly ObservableGauge<long> _outboxMaxAttemptsGauge;
    private readonly ObservableGauge<long> _outboxDeadGauge;
    private readonly Counter<long> _outboxDeadLetterCounter;
    private readonly Counter<long> _outboxCleanupCounter;
    private readonly Counter<long> _outboxDeadCleanupCounter;
    private readonly Counter<long> _outboxDeadArchiveCounter;
    private readonly Counter<long> _outboxStatsFailureCounter;
    private readonly Counter<long> _idempotencyConflictCounter;
    private readonly Counter<long> _messageRetentionDeletedCounter;
    private readonly Counter<long> _messageRetentionErrorCounter;
    private readonly ObservableGauge<double> _messageRetentionLagGauge;
    private readonly Histogram<double> _processingDuration;
    private readonly Histogram<double> _historyQueryDuration;
    private readonly Counter<long> _overloadReplyCounter;
    private readonly Counter<long> _shardFallbackCounter;
    private readonly Counter<long> _pushTriggeredCounter;

    private long _persisted;
    private long _duplicates;
    private long _receiptsApplied;
    private long _receiptDuplicates;
    private long _failures;
    private long _deadLetters;
    private long _outboxPublished;
    private long _outboxFailures;
    private long _historyQueries;
    private long _historyQueryFailures;
    private long _historyQueryQueueDepth;
    private long _historyQueriesInFlight;
    private long _outboxPending;
    private long _outboxOldestPendingAtMs = -1;
    private long _outboxOldestInFlightAtMs = -1;
    private long _outboxMaxAttempts;
    private long _outboxDead;
    private long _messageRetentionOldestPurgeableAtMs = -1;

    public RealtimeMetrics()
    {
        _persistedCounter = _meter.CreateCounter<long>("realtime.messages.persisted");
        _duplicateCounter = _meter.CreateCounter<long>("realtime.messages.duplicates");
        _receiptAppliedCounter = _meter.CreateCounter<long>("realtime.receipts.applied");
        _receiptDuplicateCounter = _meter.CreateCounter<long>("realtime.receipts.duplicates");
        _failureCounter = _meter.CreateCounter<long>("realtime.messages.failures");
        _deadLetterCounter = _meter.CreateCounter<long>("realtime.messages.dead_letters");
        _outboxPublishedCounter = _meter.CreateCounter<long>("realtime.outbox.published");
        _outboxFailureCounter = _meter.CreateCounter<long>("realtime.outbox.failures");
        _historyQueryCounter = _meter.CreateCounter<long>("realtime.history.queries");
        _historyQueryFailureCounter = _meter.CreateCounter<long>("realtime.history.failures");
        _historyQueryQueueDepthGauge = _meter.CreateObservableGauge<long>(
            "realtime.history.queue.depth",
            () => Interlocked.Read(ref _historyQueryQueueDepth));
        _historyQueriesInFlightGauge = _meter.CreateObservableGauge<long>(
            "realtime.history.in_flight",
            () => Interlocked.Read(ref _historyQueriesInFlight));
        _outboxPendingGauge = _meter.CreateObservableGauge<long>(
            "realtime.outbox.pending",
            () => Interlocked.Read(ref _outboxPending));
        _outboxOldestAgeGauge = _meter.CreateObservableGauge<double>(
            "realtime.outbox.oldest.age",
            ObserveOutboxOldestAgeSeconds,
            "s");
        _outboxOldestInFlightAgeGauge = _meter.CreateObservableGauge<double>(
            "realtime.outbox.oldest_inflight.age",
            ObserveOutboxOldestInFlightAgeSeconds,
            "s");
        _outboxMaxAttemptsGauge = _meter.CreateObservableGauge<long>(
            "realtime.outbox.max_attempts",
            () => Interlocked.Read(ref _outboxMaxAttempts));
        _outboxDeadGauge = _meter.CreateObservableGauge<long>(
            "realtime.outbox.dead",
            () => Interlocked.Read(ref _outboxDead));
        _outboxDeadLetterCounter = _meter.CreateCounter<long>(
            "realtime.outbox.dead_letters");
        _outboxCleanupCounter = _meter.CreateCounter<long>(
            "realtime.outbox.cleanup.deleted");
        _outboxDeadCleanupCounter = _meter.CreateCounter<long>(
            "realtime.outbox.cleanup.dead.deleted");
        _outboxDeadArchiveCounter = _meter.CreateCounter<long>(
            "realtime.outbox.cleanup.dead.archived");
        _outboxStatsFailureCounter = _meter.CreateCounter<long>(
            "realtime.outbox.stats.failures");
        _idempotencyConflictCounter = _meter.CreateCounter<long>(
            "realtime.messages.idempotency_conflicts");
        _messageRetentionDeletedCounter = _meter.CreateCounter<long>(
            "realtime.messages.retention.deleted");
        _messageRetentionErrorCounter = _meter.CreateCounter<long>(
            "realtime.messages.retention.errors");
        _messageRetentionLagGauge = _meter.CreateObservableGauge<double>(
            "realtime.messages.retention.lag",
            ObserveMessageRetentionLagSeconds,
            "s");
        _processingDuration = _meter.CreateHistogram<double>("realtime.messages.processing.duration", "ms");
        _historyQueryDuration = _meter.CreateHistogram<double>("realtime.history.duration", "ms");
        _overloadReplyCounter = _meter.CreateCounter<long>("realtime.overload.replies");
        _shardFallbackCounter = _meter.CreateCounter<long>("realtime.routing.shard_fallback");
        _pushTriggeredCounter = _meter.CreateCounter<long>("realtime.push.triggered");
    }

    public void RecordPersisted()
    {
        Interlocked.Increment(ref _persisted);
        _persistedCounter.Add(1);
    }

    public void RecordDuplicate()
    {
        Interlocked.Increment(ref _duplicates);
        _duplicateCounter.Add(1);
    }

    public void RecordReceiptApplied(MessageReceiptType receiptType)
    {
        Interlocked.Increment(ref _receiptsApplied);
        _receiptAppliedCounter.Add(
            1,
            new KeyValuePair<string, object?>("receipt.type", receiptType.ToString()));
    }

    public void RecordReceiptDuplicate(MessageReceiptType receiptType)
    {
        Interlocked.Increment(ref _receiptDuplicates);
        _receiptDuplicateCounter.Add(
            1,
            new KeyValuePair<string, object?>("receipt.type", receiptType.ToString()));
    }

    public void RecordProcessingFailure(string kind)
    {
        Interlocked.Increment(ref _failures);
        _failureCounter.Add(1, new KeyValuePair<string, object?>("kind", kind));
    }

    public void RecordDeadLetter(string reason)
    {
        Interlocked.Increment(ref _deadLetters);
        _deadLetterCounter.Add(1, new KeyValuePair<string, object?>("reason", reason));
    }

    public void RecordOutboxPublished()
    {
        Interlocked.Increment(ref _outboxPublished);
        _outboxPublishedCounter.Add(1);
        AdjustOutboxPending(-1);
    }

    public void RecordOutboxFailure()
    {
        Interlocked.Increment(ref _outboxFailures);
        _outboxFailureCounter.Add(1);
    }

    public void RecordOutboxDeadLetter()
    {
        _outboxDeadLetterCounter.Add(1);
        AdjustOutboxPending(-1);
        Interlocked.Increment(ref _outboxDead);
    }

    public void RecordOutboxReplay(int count)
    {
        if (count <= 0)
            return;

        AdjustOutboxPending(count);
        AdjustOutboxDead(-count);
    }

    public void RecordOutboxEnqueued(int count)
    {
        if (count > 0)
            AdjustOutboxPending(count);
    }

    public void RecordOutboxCleanup(int deleted) =>
        _outboxCleanupCounter.Add(deleted);

    /// <summary>Perf-8：物理删除 Dead 行的累计计数。</summary>
    public void RecordOutboxDeadCleanup(int deleted)
    {
        if (deleted <= 0)
            return;
        _outboxDeadCleanupCounter.Add(deleted);
        AdjustOutboxDead(-deleted);
    }

    /// <summary>Perf-8：归档 Dead 行到外部接收器的累计计数。</summary>
    public void RecordOutboxDeadArchive(int archived)
    {
        if (archived > 0)
            _outboxDeadArchiveCounter.Add(archived);
    }

    public void RecordMessageRetentionDeleted(int deleted)
    {
        if (deleted > 0)
            _messageRetentionDeletedCounter.Add(deleted);
    }

    public void RecordMessageRetentionError() =>
        _messageRetentionErrorCounter.Add(1);

    /// <summary>
    /// Lag = how far the oldest purgeable row sits behind the cutoff (seconds). 0 when caught up.
    /// </summary>
    public void UpdateMessageRetentionLag(
        long? oldestPurgeableReceivedAtMs,
        long cutoffReceivedAtMs,
        long nowMs)
    {
        if (oldestPurgeableReceivedAtMs is null || oldestPurgeableReceivedAtMs >= cutoffReceivedAtMs)
        {
            Interlocked.Exchange(ref _messageRetentionOldestPurgeableAtMs, -1);
            return;
        }

        Interlocked.Exchange(ref _messageRetentionOldestPurgeableAtMs, oldestPurgeableReceivedAtMs.Value);
        _ = nowMs;
    }

    public void RecordIdempotencyConflict() =>
        _idempotencyConflictCounter.Add(1);

    /// <summary>
    /// 低频对账：用 DB Pending/Dead 聚合覆盖进程内计数（校正 drift / oldest age）。
    /// </summary>
    public void UpdateOutboxStats(RealtimeOutboxStats stats)
    {
        Interlocked.Exchange(ref _outboxPending, stats.PendingCount);
        Interlocked.Exchange(
            ref _outboxOldestPendingAtMs,
            stats.OldestPendingAtMs ?? -1);
        Interlocked.Exchange(
            ref _outboxOldestInFlightAtMs,
            stats.OldestInFlightAtMs ?? -1);
        Interlocked.Exchange(ref _outboxMaxAttempts, stats.MaxAttemptCount);
        Interlocked.Exchange(ref _outboxDead, stats.DeadCount);
    }

    private void AdjustOutboxPending(int delta)
    {
        if (delta == 0)
            return;

        while (true)
        {
            var current = Interlocked.Read(ref _outboxPending);
            var next = Math.Max(0, current + delta);
            if (Interlocked.CompareExchange(ref _outboxPending, next, current) == current)
                return;
        }
    }

    private void AdjustOutboxDead(int delta)
    {
        if (delta == 0)
            return;

        while (true)
        {
            var current = Interlocked.Read(ref _outboxDead);
            var next = Math.Max(0, current + delta);
            if (Interlocked.CompareExchange(ref _outboxDead, next, current) == current)
                return;
        }
    }

    public void RecordOutboxStatsFailure() =>
        _outboxStatsFailureCounter.Add(1);

    public void RecordProcessingDuration(TimeSpan duration) =>
        _processingDuration.Record(duration.TotalMilliseconds);

    public void HistoryQueryEnqueued() =>
        Interlocked.Increment(ref _historyQueryQueueDepth);

    public void HistoryQueryEnqueueFailed() =>
        Interlocked.Decrement(ref _historyQueryQueueDepth);

    public void HistoryQueryStarted()
    {
        Interlocked.Decrement(ref _historyQueryQueueDepth);
        Interlocked.Increment(ref _historyQueriesInFlight);
    }

    public void RecordHistoryQuery(
        bool succeeded,
        string? reason,
        TimeSpan duration)
    {
        Interlocked.Decrement(ref _historyQueriesInFlight);
        Interlocked.Increment(ref _historyQueries);
        _historyQueryCounter.Add(1);
        _historyQueryDuration.Record(duration.TotalMilliseconds);

        if (succeeded)
            return;

        Interlocked.Increment(ref _historyQueryFailures);
        _historyQueryFailureCounter.Add(
            1,
            new KeyValuePair<string, object?>(
                "reason",
                reason ?? "unknown"));
    }

    /// <summary>
    /// 过载协议：记录一次 <c>server_busy</c> 快速失败回复。
    /// </summary>
    public void RecordOverloadReply(string queueKind, string source)
    {
        _overloadReplyCounter.Add(
            1,
            new KeyValuePair<string, object?>("queue_kind", queueKind),
            new KeyValuePair<string, object?>("source", source));
    }

    /// <summary>
    /// P0-9：记录一次分片 fallback 事件（路由目录查询失败时枚举所有活跃 shards 分别发布）。
    /// </summary>
    /// <param name="reason">fallback 原因：<c>lookup_failure</c> / <c>partial_lookup_failure</c> 等。</param>
    /// <param name="shardCount">本次 fallback 实际命中的活跃 shard 数量；为 0 表示最终回退到广播。</param>
    public void RecordShardFallback(string reason, int shardCount)
    {
        _shardFallbackCounter.Add(
            1,
            new KeyValuePair<string, object?>("reason", reason),
            new KeyValuePair<string, object?>("shard_count", Math.Max(0, shardCount)));
    }

    /// <summary>
    /// 离线推送触发计数：检测到目标用户离线并发布 <c>PushDeliveryCommand</c> 时记录。
    /// <paramref name="isMention"/> 作为低基数标签区分 @mention 推送与普通推送。
    /// </summary>
    public void RecordPushTriggered(bool isMention)
    {
        _pushTriggeredCounter.Add(
            1,
            new KeyValuePair<string, object?>("is_mention", isMention.ToString()));
    }

    public RealtimeMetricsSnapshot GetSnapshot() => new(
        Interlocked.Read(ref _persisted),
        Interlocked.Read(ref _duplicates),
        Interlocked.Read(ref _receiptsApplied),
        Interlocked.Read(ref _receiptDuplicates),
        Interlocked.Read(ref _failures),
        Interlocked.Read(ref _deadLetters),
        Interlocked.Read(ref _outboxPublished),
        Interlocked.Read(ref _outboxFailures),
        Interlocked.Read(ref _historyQueries),
        Interlocked.Read(ref _historyQueryFailures),
        Interlocked.Read(ref _historyQueryQueueDepth),
        Interlocked.Read(ref _historyQueriesInFlight),
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

    public void Dispose() => _meter.Dispose();

    private double ObserveOutboxOldestAgeSeconds() =>
        ObserveAgeSeconds(Interlocked.Read(ref _outboxOldestPendingAtMs));

    private double ObserveOutboxOldestInFlightAgeSeconds() =>
        ObserveAgeSeconds(Interlocked.Read(ref _outboxOldestInFlightAtMs));

    private double ObserveMessageRetentionLagSeconds()
    {
        var oldest = Interlocked.Read(ref _messageRetentionOldestPurgeableAtMs);
        if (oldest < 0)
            return 0;

        // Lag relative to "now": how old the oldest still-purgeable row is.
        return Math.Max(
            0,
            (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - oldest) / 1000d);
    }

    private static double ObserveAgeSeconds(long oldestAtMs)
    {
        if (oldestAtMs < 0)
            return 0;

        return Math.Max(
            0,
            (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - oldestAtMs) / 1000d);
    }
}

public sealed record RealtimeMetricsSnapshot(
    long Persisted,
    long Duplicates,
    long ReceiptsApplied,
    long ReceiptDuplicates,
    long Failures,
    long DeadLetters,
    long OutboxPublished,
    long OutboxFailures,
    long HistoryQueries,
    long HistoryQueryFailures,
    long HistoryQueryQueueDepth,
    long HistoryQueriesInFlight,
    long GeneratedAtMs);
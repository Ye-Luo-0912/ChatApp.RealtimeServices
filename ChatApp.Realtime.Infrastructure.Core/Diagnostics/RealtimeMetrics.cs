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
    private readonly ObservableGauge<long> _outboxMaxAttemptsGauge;
    private readonly Counter<long> _outboxStatsFailureCounter;
    private readonly Histogram<double> _processingDuration;
    private readonly Histogram<double> _historyQueryDuration;

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
    private long _outboxMaxAttempts;

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
        _outboxMaxAttemptsGauge = _meter.CreateObservableGauge<long>(
            "realtime.outbox.max_attempts",
            () => Interlocked.Read(ref _outboxMaxAttempts));
        _outboxStatsFailureCounter = _meter.CreateCounter<long>(
            "realtime.outbox.stats.failures");
        _processingDuration = _meter.CreateHistogram<double>("realtime.messages.processing.duration", "ms");
        _historyQueryDuration = _meter.CreateHistogram<double>("realtime.history.duration", "ms");
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
    }

    public void RecordOutboxFailure()
    {
        Interlocked.Increment(ref _outboxFailures);
        _outboxFailureCounter.Add(1);
    }

    public void UpdateOutboxStats(RealtimeOutboxStats stats)
    {
        Interlocked.Exchange(ref _outboxPending, stats.PendingCount);
        Interlocked.Exchange(
            ref _outboxOldestPendingAtMs,
            stats.OldestPendingAtMs ?? -1);
        Interlocked.Exchange(ref _outboxMaxAttempts, stats.MaxAttemptCount);
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

    private double ObserveOutboxOldestAgeSeconds()
    {
        var oldestPendingAtMs = Interlocked.Read(ref _outboxOldestPendingAtMs);
        if (oldestPendingAtMs < 0)
            return 0;

        return Math.Max(
            0,
            (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - oldestPendingAtMs) / 1000d);
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
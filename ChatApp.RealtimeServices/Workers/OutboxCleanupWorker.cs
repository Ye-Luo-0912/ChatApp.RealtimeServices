using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 按 TTL 批量删除已发布与死信 Outbox 行，避免无限膨胀。
/// Perf-8：增加 Published 批次节流（最大批次数 + 批间 sleep），避免大积压时持续产生 WAL/Vacuum 压力。
/// Perf-8：Dead 行按 TTL 归档（<see cref="IDeadLetterArchiveSink"/>）后物理删除。
/// </summary>
public sealed class OutboxCleanupWorker : BackgroundService
{
    private readonly IRealtimeOutboxStore _outboxStore;
    private readonly IDeadLetterArchiveSink _archiveSink;
    private readonly RealtimeMetrics _metrics;
    private readonly OutboxOptions _options;
    private readonly TimeSpan _interval;
    private readonly ILogger<OutboxCleanupWorker> _logger;

    public OutboxCleanupWorker(
        IRealtimeOutboxStore outboxStore,
        IDeadLetterArchiveSink archiveSink,
        RealtimeMetrics metrics,
        IOptions<OutboxOptions> options,
        ILogger<OutboxCleanupWorker> logger)
    {
        _outboxStore = outboxStore;
        _archiveSink = archiveSink;
        _metrics = metrics;
        _options = options.Value;
        _interval = TimeSpan.FromMilliseconds(Math.Max(1_000, _options.CleanupIntervalMs));
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);
        do
        {
            try
            {
                await CleanupPublishedAsync(stoppingToken).ConfigureAwait(false);
                await CleanupDeadAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Outbox 清理失败，将在下一周期重试。");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    private async Task CleanupPublishedAsync(CancellationToken stoppingToken)
    {
        if (_options.PublishedRetentionHours <= 0)
            return;

        var cutoff = DateTimeOffset.UtcNow
            .AddHours(-_options.PublishedRetentionHours)
            .ToUnixTimeMilliseconds();
        var total = 0;
        var batchIndex = 0;
        var sleepMs = Math.Max(0, _options.PublishedBatchSleepMs);
        while (!stoppingToken.IsCancellationRequested)
        {
            // Perf-8：限制单周期最大批次数，避免大积压时持续 DELETE 拖垮 WAL/Vacuum。
            if (_options.PublishedMaxBatchesPerCycle > 0
                && batchIndex >= _options.PublishedMaxBatchesPerCycle)
            {
                _logger.LogInformation(
                    "Outbox 已发布行清理达到单周期最大批次数。批次={Batches}；删除行数={Deleted}；保留小时={RetentionHours}",
                    batchIndex,
                    total,
                    _options.PublishedRetentionHours);
                break;
            }

            var deleted = await _outboxStore
                .CleanupPublishedAsync(cutoff, _options.CleanupBatchSize, stoppingToken)
                .ConfigureAwait(false);
            if (deleted <= 0)
                break;

            total += deleted;
            batchIndex++;
            _metrics.RecordOutboxCleanup(deleted);

            if (sleepMs > 0)
                await Task.Delay(sleepMs, stoppingToken).ConfigureAwait(false);
        }

        if (total > 0)
        {
            _logger.LogInformation(
                "已清理过期已发布 Outbox。删除行数={Deleted}；批次={Batches}；保留小时={RetentionHours}",
                total,
                batchIndex,
                _options.PublishedRetentionHours);
        }
    }

    private async Task CleanupDeadAsync(CancellationToken stoppingToken)
    {
        // DeadRetentionDays=0 表示不按 TTL 清理 Dead 行（仍可由运维手工 Replay）。
        if (_options.DeadRetentionDays <= 0)
            return;

        var cutoff = DateTimeOffset.UtcNow
            .AddDays(-_options.DeadRetentionDays)
            .ToUnixTimeMilliseconds();

        // 单周期最多处理 DeadMaxRows 行；0 表示不限制（按 batchSize 逐步推进）。
        var remaining = _options.DeadMaxRows > 0
            ? _options.DeadMaxRows
            : int.MaxValue;
        var batchSize = Math.Max(1, _options.CleanupBatchSize);

        var archivedTotal = 0;
        var deletedTotal = 0;
        while (!stoppingToken.IsCancellationRequested && remaining > 0)
        {
            var limit = Math.Min(batchSize, remaining);
            var dead = await _outboxStore
                .ListDeadAsync(cutoff, limit, stoppingToken)
                .ConfigureAwait(false);
            if (dead.Count == 0)
                break;

            // 归档（即使是 NullDeadLetterArchiveSink 也会返回所有 event_id）。
            // 任何抛出错误会冒泡到 ExecuteAsync 的 catch，下一周期重试。
            var archivedIds = await _archiveSink
                .ArchiveAsync(dead, stoppingToken)
                .ConfigureAwait(false);
            if (archivedIds.Count == 0)
            {
                _logger.LogWarning(
                    "Outbox Dead 归档返回 0 行成功，跳过本批次物理删除。批次大小={BatchSize}；cutoff(ms)={Cutoff}",
                    dead.Count,
                    cutoff);
                break;
            }

            _metrics.RecordOutboxDeadArchive(archivedIds.Count);
            archivedTotal += archivedIds.Count;

            var deleted = await _outboxStore
                .DeleteDeadBatchAsync(archivedIds, stoppingToken)
                .ConfigureAwait(false);
            if (deleted > 0)
            {
                _metrics.RecordOutboxDeadCleanup(deleted);
                deletedTotal += deleted;
            }

            remaining -= dead.Count;

            // 当前批次未填满 limit，说明没有更多 Dead 行了。
            if (dead.Count < limit)
                break;
        }

        if (archivedTotal > 0 || deletedTotal > 0)
        {
            _logger.LogInformation(
                "已归档并清理过期 Dead Outbox。归档={Archived}；删除={Deleted}；保留天数={RetentionDays}",
                archivedTotal,
                deletedTotal,
                _options.DeadRetentionDays);
        }
    }
}

using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 按 TTL 批量删除已发布 Outbox 行，避免无限膨胀。
/// </summary>
public sealed class OutboxCleanupWorker : BackgroundService
{
    private readonly IRealtimeOutboxStore _outboxStore;
    private readonly RealtimeMetrics _metrics;
    private readonly OutboxOptions _options;
    private readonly TimeSpan _interval;
    private readonly ILogger<OutboxCleanupWorker> _logger;

    public OutboxCleanupWorker(
        IRealtimeOutboxStore outboxStore,
        RealtimeMetrics metrics,
        IOptions<OutboxOptions> options,
        ILogger<OutboxCleanupWorker> logger)
    {
        _outboxStore = outboxStore;
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
                if (_options.PublishedRetentionHours <= 0)
                    continue;

                var cutoff = DateTimeOffset.UtcNow
                    .AddHours(-_options.PublishedRetentionHours)
                    .ToUnixTimeMilliseconds();
                var total = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var deleted = await _outboxStore
                        .CleanupPublishedAsync(cutoff, _options.CleanupBatchSize, stoppingToken)
                        .ConfigureAwait(false);
                    if (deleted <= 0)
                        break;
                    total += deleted;
                    _metrics.RecordOutboxCleanup(deleted);
                }

                if (total > 0)
                {
                    _logger.LogInformation(
                        "已清理过期已发布 Outbox。删除行数={Deleted}；保留小时={RetentionHours}",
                        total,
                        _options.PublishedRetentionHours);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Outbox 已发布行清理失败，将在下一周期重试。");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }
}

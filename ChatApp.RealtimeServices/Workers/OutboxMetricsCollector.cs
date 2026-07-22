using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

public sealed class OutboxMetricsCollector : BackgroundService
{
    private readonly IRealtimeOutboxStore _outboxStore;
    private readonly RealtimeMetrics _metrics;
    private readonly TimeSpan _collectionInterval;
    private readonly ILogger<OutboxMetricsCollector> _logger;

    public OutboxMetricsCollector(
        IRealtimeOutboxStore outboxStore,
        RealtimeMetrics metrics,
        IOptions<ObservabilityOptions> options,
        ILogger<OutboxMetricsCollector> logger)
    {
        _outboxStore = outboxStore;
        _metrics = metrics;
        _collectionInterval = TimeSpan.FromMilliseconds(
            options.Value.OutboxStatsCollectionIntervalMs);
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_collectionInterval);
        do
        {
            try
            {
                var stats = await _outboxStore
                    .GetStatsAsync(stoppingToken)
                    .ConfigureAwait(false);
                _metrics.UpdateOutboxStats(stats);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _metrics.RecordOutboxStatsFailure();
                _logger.LogWarning(ex, "采集 Outbox 指标失败，将在下一周期重试。");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }
}

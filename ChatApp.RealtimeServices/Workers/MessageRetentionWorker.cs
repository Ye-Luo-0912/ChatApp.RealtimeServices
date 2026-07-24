using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// Batched age-based hard-delete of messages older than the retention horizon.
/// Silent GC (no ConversationChanged). Disabled when Enabled=false or effective horizon is 0.
/// </summary>
public sealed class MessageRetentionWorker : BackgroundService
{
    private readonly IRealtimeMessageRetentionStore _store;
    private readonly RealtimeMetrics _metrics;
    private readonly MessageRetentionOptions _options;
    private readonly SyncBootstrapOptions _syncBootstrap;
    private readonly TimeSpan _interval;
    private readonly ILogger<MessageRetentionWorker> _logger;

    public MessageRetentionWorker(
        IRealtimeMessageRetentionStore store,
        RealtimeMetrics metrics,
        IOptions<MessageRetentionOptions> options,
        SyncBootstrapOptions syncBootstrap,
        ILogger<MessageRetentionWorker> logger)
    {
        _store = store;
        _metrics = metrics;
        _options = options.Value;
        _syncBootstrap = syncBootstrap;
        _interval = TimeSpan.FromMilliseconds(Math.Max(1_000, _options.IntervalMs));
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);
        do
        {
            try
            {
                await RunCycleAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _metrics.RecordMessageRetentionError();
                _logger.LogWarning(ex, "Message retention GC cycle failed; will retry next interval.");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    private async Task RunCycleAsync(CancellationToken ct)
    {
        var horizonMs = _options.ResolveEffectiveHorizonMs(_syncBootstrap.RetentionHorizonMs);
        if (!_options.Enabled || horizonMs <= 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var cutoff = now - horizonMs;
        if (cutoff <= 0)
            return;

        var totalDeleted = 0;
        var batches = 0;
        var maxBatches = _options.MaxBatchesPerCycle <= 0
            ? int.MaxValue
            : _options.MaxBatchesPerCycle;
        var batchSize = Math.Max(1, _options.BatchSize);
        var sleep = TimeSpan.FromMilliseconds(Math.Max(0, _options.BatchSleepMs));

        while (!ct.IsCancellationRequested && batches < maxBatches)
        {
            var result = await _store
                .TryPurgeBatchAsync(cutoff, batchSize, ct)
                .ConfigureAwait(false);

            if (!result.LockAcquired)
            {
                _logger.LogDebug("Message retention GC skipped: advisory lock held by another instance.");
                break;
            }

            if (result.DeletedCount <= 0)
                break;

            totalDeleted += result.DeletedCount;
            _metrics.RecordMessageRetentionDeleted(result.DeletedCount);
            batches++;

            if (sleep > TimeSpan.Zero && batches < maxBatches)
                await Task.Delay(sleep, ct).ConfigureAwait(false);
        }

        try
        {
            var stats = await _store.GetPurgeableStatsAsync(cutoff, ct).ConfigureAwait(false);
            _metrics.UpdateMessageRetentionLag(
                stats.OldestReceivedAtMs,
                cutoff,
                now);
        }
        catch (Exception ex)
        {
            _metrics.RecordMessageRetentionError();
            _logger.LogDebug(ex, "Message retention lag stats failed.");
        }

        if (totalDeleted > 0)
        {
            _logger.LogInformation(
                "Message retention GC deleted rows. Deleted={Deleted}; Batches={Batches}; HorizonMs={HorizonMs}; Cutoff={Cutoff}",
                totalDeleted,
                batches,
                horizonMs,
                cutoff);
        }
    }
}

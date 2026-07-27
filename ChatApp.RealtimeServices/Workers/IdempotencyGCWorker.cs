using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// LongTerm-1：独立命令幂等账本 + 用户删除 tombstone 的周期 GC。
/// <para>
/// 保留期由 <see cref="IdempotencyOptions"/> 控制，启动时校验不少于 JetStream MaxAge。
/// 关闭时（Enabled=false）Worker 空转，但账本写入仍由 Incoming Processor 执行。
/// </para>
/// <para>
/// 不参与就绪检查（清理类 Worker 不阻断 readiness）。
/// </para>
/// </summary>
public sealed class IdempotencyGCWorker : BackgroundService
{
    private readonly ICommandIdempotencyLedger _ledger;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;
    private readonly IdempotencyOptions _options;
    private readonly long _jetStreamMaxAgeMs;
    private readonly TimeSpan _interval;
    private readonly ILogger<IdempotencyGCWorker> _logger;

    public IdempotencyGCWorker(
        ICommandIdempotencyLedger ledger,
        IUserDeletionTombstoneStore tombstoneStore,
        IOptions<IdempotencyOptions> options,
        IOptions<NatsOptions> natsOptions,
        ILogger<IdempotencyGCWorker> logger)
    {
        _ledger = ledger;
        _tombstoneStore = tombstoneStore;
        _options = options.Value;
        _jetStreamMaxAgeMs = (natsOptions.Value.JetStream?.MaxAgeHours ?? 168) * 3_600_000L;
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
                _logger.LogWarning(ex, "Idempotency GC cycle failed; will retry next interval.");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    private async Task RunCycleAsync(CancellationToken ct)
    {
        if (!_options.Enabled)
            return;

        var horizonMs = _options.ResolveEffectiveHorizonMs(_jetStreamMaxAgeMs);
        if (horizonMs <= 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var cutoff = now - horizonMs;
        if (cutoff <= 0)
            return;

        var batchSize = Math.Max(1, _options.BatchSize);
        var sleep = TimeSpan.FromMilliseconds(Math.Max(0, _options.BatchSleepMs));
        var maxBatches = _options.MaxBatchesPerCycle <= 0
            ? int.MaxValue
            : _options.MaxBatchesPerCycle;

        var totalLedgerPurged = 0L;
        var totalTombstonesPurged = 0L;
        var batches = 0;

        while (!ct.IsCancellationRequested && batches < maxBatches)
        {
            var ledgerDeleted = await _ledger
                .PurgeOlderThanAsync(cutoff, batchSize, ct)
                .ConfigureAwait(false);

            // LongTerm-1：tombstone 保留期与账本一致（>= JetStream MaxAge）。
            // tombstone 早于账本清理会导致已注销用户的旧命令在 cutoff 后"复活"窗口扩大。
            var tombstoneDeleted = await _tombstoneStore
                .PurgeOlderThanAsync(cutoff, batchSize, ct)
                .ConfigureAwait(false);

            if (ledgerDeleted == 0 && tombstoneDeleted == 0)
                break;

            totalLedgerPurged += ledgerDeleted;
            totalTombstonesPurged += tombstoneDeleted;
            batches++;

            if (sleep > TimeSpan.Zero && batches < maxBatches)
                await Task.Delay(sleep, ct).ConfigureAwait(false);
        }

        if (totalLedgerPurged > 0 || totalTombstonesPurged > 0)
        {
            _logger.LogInformation(
                "Idempotency GC completed. LedgerPurged={Ledger}; TombstonesPurged={Tombstones}; " +
                "Batches={Batches}; HorizonMs={HorizonMs}; Cutoff={Cutoff}",
                totalLedgerPurged,
                totalTombstonesPurged,
                batches,
                horizonMs,
                cutoff);
        }
    }
}

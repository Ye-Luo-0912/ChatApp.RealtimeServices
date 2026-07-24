namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// Age-based hard-delete GC for message rows. Aligns with
/// <c>SyncBootstrap:RetentionHorizonMs</c> / <c>BeyondRetention</c>: GC removes rows older than the
/// same horizon window; sync invalidates cursors older than <c>tip − horizon</c>.
/// </summary>
public sealed class MessageRetentionOptions
{
    public const string SectionName = "MessageRetention";

    /// <summary>Master switch. When false the worker is idle even if a horizon is configured.</summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Retention window in milliseconds. Messages with <c>received_at_ms &lt; now − horizon</c>
    /// are purge candidates. When 0, falls back to <c>SyncBootstrap:RetentionHorizonMs</c>,
    /// then to <see cref="RetentionDays"/> (days → ms). Effective 0 disables GC.
    /// </summary>
    public long RetentionHorizonMs { get; init; }

    /// <summary>
    /// Convenience alternative when <see cref="RetentionHorizonMs"/> is 0 and SyncBootstrap
    /// horizon is also 0. Ignored when either ms horizon is set.
    /// </summary>
    public int RetentionDays { get; init; }

    public int BatchSize { get; init; } = 500;

    /// <summary>Worker poll interval between purge cycles.</summary>
    public int IntervalMs { get; init; } = 60_000;

    /// <summary>Sleep between batches within one cycle (online-safe throttling).</summary>
    public int BatchSleepMs { get; init; } = 100;

    /// <summary>Max delete batches per cycle (0 = unlimited until a batch returns 0).</summary>
    public int MaxBatchesPerCycle { get; init; } = 100;

    /// <summary>
    /// Resolves the effective horizon: explicit ms → SyncBootstrap ms → RetentionDays → 0.
    /// </summary>
    public long ResolveEffectiveHorizonMs(long syncBootstrapRetentionHorizonMs)
    {
        if (RetentionHorizonMs > 0)
            return RetentionHorizonMs;
        if (syncBootstrapRetentionHorizonMs > 0)
            return syncBootstrapRetentionHorizonMs;
        if (RetentionDays > 0)
            return RetentionDays * 86_400_000L;
        return 0;
    }

    public bool IsEffectivelyEnabled(long syncBootstrapRetentionHorizonMs) =>
        Enabled && ResolveEffectiveHorizonMs(syncBootstrapRetentionHorizonMs) > 0;
}

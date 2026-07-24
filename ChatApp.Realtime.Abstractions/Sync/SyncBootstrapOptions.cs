namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>Sync bootstrap / catch-up policy knobs.</summary>
public sealed class SyncBootstrapOptions
{
    public const string SectionName = "SyncBootstrap";

    /// <summary>
    /// When &gt; 0, an otherwise-valid cursor whose tip gap (<c>tipAt - afterAt</c>) exceeds this many
    /// milliseconds yields <see cref="SyncCursorResetReason.GapTooLarge"/>. Default 0 = disabled.
    /// </summary>
    public long MaxCatchUpGapMs { get; init; }

    /// <summary>
    /// When &gt; 0, a cursor older than <c>tipAt - RetentionHorizonMs</c> yields
    /// <see cref="SyncCursorResetReason.BeyondRetention"/> (expired history), distinct from
    /// <see cref="SyncCursorResetReason.MessageNotFound"/> (random/deleted id).
    /// Default 0 = disabled. Age-based GC (<c>MessageRetention</c>) hard-deletes rows older than
    /// the same window (<c>received_at_ms &lt; now − horizon</c>); purged ids surface as
    /// MessageNotFound and are reclassified to BeyondRetention when the client watermark is
    /// older than tip − this horizon. Prefer setting one shared horizon (SyncBootstrap and/or
    /// MessageRetention:RetentionHorizonMs / RetentionDays).
    /// </summary>
    public long RetentionHorizonMs { get; init; }
}

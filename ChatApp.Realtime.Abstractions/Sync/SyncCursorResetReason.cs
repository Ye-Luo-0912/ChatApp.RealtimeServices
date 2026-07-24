namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>
/// Why a client sync cursor / device watermark requires full local reset for a conversation.
/// </summary>
/// <remarks>
/// Message retention / tombstone horizon invalidation is not implemented yet (no sync retention
/// horizon in the store). When added, prefer a new reason rather than overloading
/// <see cref="MessageNotFound"/>.
/// </remarks>
public enum SyncCursorResetReason : byte
{
    /// <summary>Cursor message id is missing in the conversation (deleted, purged, or random).</summary>
    MessageNotFound = 1,

    /// <summary>Cursor is strictly ahead of the conversation tip (future / fabricated watermark).</summary>
    AheadOfTip = 2,

    /// <summary>Client watermark conversation is no longer a membership for the user.</summary>
    MembershipLost = 3,

    /// <summary>
    /// Valid cursor but tip gap exceeds <c>SyncBootstrap:MaxCatchUpGapMs</c> (0 disables).
    /// </summary>
    GapTooLarge = 4,

    /// <summary>
    /// Cursor is older than <c>SyncBootstrap:RetentionHorizonMs</c> relative to tip (0 disables).
    /// Distinct from <see cref="MessageNotFound"/> (random/deleted id).
    /// </summary>
    BeyondRetention = 5
}

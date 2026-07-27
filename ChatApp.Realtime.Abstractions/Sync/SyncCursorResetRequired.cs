namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>
/// Per-conversation signal that the client must wipe local cache for this conversation and full-resync.
/// </summary>
public sealed class SyncCursorResetRequired
{
    public required string ConversationId { get; init; }
    public required SyncCursorResetReason Reason { get; init; }

    /// <summary>Server tip when known; null when conversation has no tip or membership is lost.</summary>
    public long? TipChangedAtMs { get; init; }

    public string? TipMessageId { get; init; }

    /// <summary>Client-supplied (or device-stored) watermark that was rejected.</summary>
    public long? ClientAfterChangedAtMs { get; init; }

    public string? ClientAfterMessageId { get; init; }
}

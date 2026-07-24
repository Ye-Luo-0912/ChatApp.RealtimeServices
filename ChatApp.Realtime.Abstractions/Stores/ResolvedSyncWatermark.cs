namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Result of resolving a client sync watermark against conversation history / tip.
/// </summary>
public sealed class ResolvedSyncWatermark
{
    public required string ConversationId { get; init; }

    /// <summary>
    /// Query watermark when <see cref="IsValid"/>; otherwise typically the tip (hint only — do not
    /// treat as a successful incremental catch-up cursor).
    /// </summary>
    public required long AfterReceivedAtMs { get; init; }

    public required string AfterMessageId { get; init; }

    /// <summary>True when the client cursor exists in-conversation and is not ahead of tip.</summary>
    public bool IsValid { get; init; }

    /// <summary>Set when <see cref="IsValid"/> is false (message missing or ahead of tip).</summary>
    public SyncWatermarkInvalidationKind? InvalidationKind { get; init; }

    public long? TipReceivedAtMs { get; init; }

    public string? TipMessageId { get; init; }

    public long ClientAfterReceivedAtMs { get; init; }

    public string ClientAfterMessageId { get; init; } = string.Empty;
}

/// <summary>Store-level watermark invalidation (membership / gap are applied by the processor).</summary>
public enum SyncWatermarkInvalidationKind : byte
{
    MessageNotFound = 1,
    AheadOfTip = 2,

    /// <summary>Cursor predates the configured retention horizon (processor-computed; stores may also set this).</summary>
    BeyondRetention = 3
}

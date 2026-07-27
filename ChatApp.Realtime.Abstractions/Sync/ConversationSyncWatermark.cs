namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class ConversationSyncWatermark
{
    public required string ConversationId { get; init; }
    /// <summary>
    /// Reliability-1：变更水位（changed_at_ms），用于增量追赶过滤。
    /// </summary>
    public long AfterChangedAtMs { get; init; }
    public required string AfterMessageId { get; init; }
}

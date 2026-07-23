namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class DeviceSyncCursor
{
    public required string ConversationId { get; init; }
    public long AfterReceivedAtMs { get; init; }
    public required string AfterMessageId { get; init; }
}

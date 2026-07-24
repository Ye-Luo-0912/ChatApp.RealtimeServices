namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>跨 Gateway Typing 扇出（NATS Core，非 JetStream/Outbox）。</summary>
public sealed class EphemeralTypingEvent
{
    public required string OriginInstanceId { get; init; }
    public long SenderUserId { get; init; }
    public long TargetUserId { get; init; }
    public string? ConversationId { get; init; }
    public bool IsTyping { get; init; }
}

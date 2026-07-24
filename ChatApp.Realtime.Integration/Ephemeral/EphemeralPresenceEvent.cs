namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>跨 Gateway Presence 变更（NATS Core，非 JetStream/Outbox）。</summary>
public sealed class EphemeralPresenceEvent
{
    public required string OriginInstanceId { get; init; }
    public long UserId { get; init; }
    public bool IsOnline { get; init; }
}

using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Integration.Outbox;

public sealed class RealtimeIntegrationOutboxItem
{
    public required string EventId { get; init; }
    public required string PayloadJson { get; init; }
    public required long TargetUserId { get; init; }
    public required short EventType { get; init; }
    public short Status { get; set; }
    public long CreatedAtMs { get; init; }
    public long NextAttemptAtMs { get; init; }
    public long? PublishedAtMs { get; set; }
    public int AttemptCount { get; set; }
    public string? LockedBy { get; set; }
    public long? LockedUntilMs { get; set; }
    public string? LastError { get; set; }

    public static RealtimeIntegrationOutboxItem FromEvent(RealtimeEvent evt)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return new RealtimeIntegrationOutboxItem
        {
            EventId = evt.EventId,
            PayloadJson = RealtimeWireSerializer.Serialize(evt),
            TargetUserId = evt.TargetUserId,
            EventType = (short)evt.Type,
            CreatedAtMs = now,
            NextAttemptAtMs = now
        };
    }
}

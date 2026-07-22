namespace ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;

public sealed class RealtimeOutboxEntity
{
    public required string EventId { get; init; }
    public required string PayloadJson { get; init; }
    public required long TargetUserId { get; init; }
    public required short EventType { get; init; }
    public long CreatedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public long NextAttemptAtMs { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public long? PublishedAtMs { get; set; }
    public int AttemptCount { get; set; }
    public string? LockedBy { get; set; }
    public long? LockedUntilMs { get; set; }
    public string? LastError { get; set; }
}

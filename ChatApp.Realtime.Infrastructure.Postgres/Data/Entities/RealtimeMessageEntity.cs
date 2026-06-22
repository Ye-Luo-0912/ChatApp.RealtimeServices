namespace ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;

public sealed class RealtimeMessageEntity
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }
    public long ReceivedAtMs { get; init; }
    public long CreatedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

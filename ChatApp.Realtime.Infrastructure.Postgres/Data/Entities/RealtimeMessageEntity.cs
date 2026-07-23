namespace ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;

public sealed class RealtimeMessageEntity
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }
    public string? ContentFingerprint { get; init; }
    public string? ConversationId { get; init; }
    public long ReceivedAtMs { get; init; }
    public long? DeliveredAtMs { get; set; }
    public long? ReadAtMs { get; set; }
    public string? ReplyToMessageId { get; init; }
    public long? ReplyToSenderUserId { get; init; }
    public string? ReplyToPreview { get; init; }
    public string? ForwardedFromMessageId { get; init; }
    public long? ForwardedFromSenderUserId { get; init; }
    public string? ForwardedFromPreview { get; init; }
    public long? RecalledAtMs { get; set; }
    public long CreatedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

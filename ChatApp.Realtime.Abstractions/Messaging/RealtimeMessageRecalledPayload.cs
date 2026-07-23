namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeMessageRecalledPayload
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public long RecalledAtMs { get; init; }
}

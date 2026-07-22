namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeMessageReceiptPayload
{
    public required string MessageId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required MessageReceiptType ReceiptType { get; init; }
    public long OccurredAtMs { get; init; }
}
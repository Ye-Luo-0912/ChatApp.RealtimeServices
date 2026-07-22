using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class MessageReceiptRecord
{
    public required string CommandId { get; init; }
    public required string MessageId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string ReceiverSessionId { get; init; }
    public required MessageReceiptType ReceiptType { get; init; }
    public long OccurredAtMs { get; init; }
}
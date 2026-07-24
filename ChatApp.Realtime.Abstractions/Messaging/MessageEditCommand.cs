namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditCommand
{
    public required string RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Content { get; init; }
    public long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public long OccurredAtMs { get; init; }
}

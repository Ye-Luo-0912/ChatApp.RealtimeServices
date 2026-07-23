namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageRecallCommand
{
    public required string RequestId { get; init; }
    public required string MessageId { get; init; }
    public long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public long OccurredAtMs { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class IncomingMessageCommand
{
    public required string CommandId { get; init; }
    public required string ClientMessageId { get; init; }

    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }

    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }

    public long ReceivedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

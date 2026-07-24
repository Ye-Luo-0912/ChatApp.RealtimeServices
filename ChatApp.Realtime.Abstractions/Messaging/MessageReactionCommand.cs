namespace ChatApp.Realtime.Abstractions.Messaging;

public enum MessageReactionAction : byte
{
    Add = 1,
    Remove = 2
}

public sealed class MessageReactionCommand
{
    public required string RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Emoji { get; init; }
    public MessageReactionAction Action { get; init; }
    public long ActorUserId { get; init; }
    public required string ActorSessionId { get; init; }
    public long OccurredAtMs { get; init; }
}

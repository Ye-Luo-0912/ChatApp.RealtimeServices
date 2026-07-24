namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class MessageReactionRecord
{
    public required string MessageId { get; init; }
    public long UserId { get; init; }
    public required string Emoji { get; init; }
    public long CreatedAtMs { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Stores;

public enum MessageReactionPersistStatus
{
    Applied = 1,
    Unchanged = 2,
    NotFound = 3,
    NotAllowed = 4,
    AlreadyRecalled = 5,
    LimitExceeded = 6
}

public sealed record MessageReactionPersistResult(
    MessageReactionPersistStatus Status,
    string MessageId,
    string? ConversationId = null,
    string? Emoji = null,
    long? OccurredAtMs = null,
    int? EmojiCount = null);

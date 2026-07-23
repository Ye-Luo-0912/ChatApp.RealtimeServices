namespace ChatApp.Realtime.Abstractions.Stores;

public enum MessageRecallPersistStatus
{
    Applied = 1,
    Unchanged = 2,
    NotFound = 3,
    NotAllowed = 4,
    WindowExpired = 5
}

public sealed record MessageRecallPersistResult(
    MessageRecallPersistStatus Status,
    string MessageId,
    long? ReceiverUserId = null,
    string? ConversationId = null,
    long? RecalledAtMs = null);

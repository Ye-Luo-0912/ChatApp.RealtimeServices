namespace ChatApp.Realtime.Abstractions.Stores;

public enum MessageEditPersistStatus
{
    Applied = 1,
    Unchanged = 2,
    NotFound = 3,
    NotAllowed = 4,
    WindowExpired = 5,
    AlreadyRecalled = 6,
    RequestConflict = 7
}

public sealed record MessageEditPersistResult(
    MessageEditPersistStatus Status,
    string MessageId,
    long? ReceiverUserId = null,
    string? ConversationId = null,
    string? Content = null,
    int? EditVersion = null,
    long? EditedAtMs = null);

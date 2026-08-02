namespace ChatApp.Realtime.Abstractions.Stores;

public sealed record RealtimeMessagePersistResult(
    RealtimeMessagePersistKind Kind,
    string MessageId,
    long? ConversationSequence = null)
{
    public bool IsNew => Kind == RealtimeMessagePersistKind.Created;
    public bool IsConflict => Kind == RealtimeMessagePersistKind.ContentConflict;
    public bool IsAttachmentBindFailed => Kind == RealtimeMessagePersistKind.AttachmentBindFailed;
    public bool IsNotAllowed => Kind == RealtimeMessagePersistKind.NotAllowed;
    public bool IsUserDeleted => Kind == RealtimeMessagePersistKind.UserDeleted;
    public bool IsUserFrozen => Kind == RealtimeMessagePersistKind.UserFrozen;

    public static RealtimeMessagePersistResult Created(string messageId, long? conversationSequence = null) =>
        new(RealtimeMessagePersistKind.Created, messageId, conversationSequence);

    public static RealtimeMessagePersistResult Duplicate(string messageId) =>
        new(RealtimeMessagePersistKind.Duplicate, messageId);

    public static RealtimeMessagePersistResult Conflict(string messageId) =>
        new(RealtimeMessagePersistKind.ContentConflict, messageId);

    public static RealtimeMessagePersistResult AttachmentBindFailed(string messageId) =>
        new(RealtimeMessagePersistKind.AttachmentBindFailed, messageId);

    public static RealtimeMessagePersistResult NotAllowed(string messageId) =>
        new(RealtimeMessagePersistKind.NotAllowed, messageId);

    public static RealtimeMessagePersistResult UserDeleted(string messageId) =>
        new(RealtimeMessagePersistKind.UserDeleted, messageId);

    public static RealtimeMessagePersistResult UserFrozen(string messageId) =>
        new(RealtimeMessagePersistKind.UserFrozen, messageId);
}

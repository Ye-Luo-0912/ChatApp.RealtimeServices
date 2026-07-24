namespace ChatApp.Realtime.Abstractions.Stores;

public sealed record RealtimeMessagePersistResult(
    RealtimeMessagePersistKind Kind,
    string MessageId)
{
    public bool IsNew => Kind == RealtimeMessagePersistKind.Created;
    public bool IsConflict => Kind == RealtimeMessagePersistKind.ContentConflict;
    public bool IsAttachmentBindFailed => Kind == RealtimeMessagePersistKind.AttachmentBindFailed;
    public bool IsNotAllowed => Kind == RealtimeMessagePersistKind.NotAllowed;

    public static RealtimeMessagePersistResult Created(string messageId) =>
        new(RealtimeMessagePersistKind.Created, messageId);

    public static RealtimeMessagePersistResult Duplicate(string messageId) =>
        new(RealtimeMessagePersistKind.Duplicate, messageId);

    public static RealtimeMessagePersistResult Conflict(string messageId) =>
        new(RealtimeMessagePersistKind.ContentConflict, messageId);

    public static RealtimeMessagePersistResult AttachmentBindFailed(string messageId) =>
        new(RealtimeMessagePersistKind.AttachmentBindFailed, messageId);

    public static RealtimeMessagePersistResult NotAllowed(string messageId) =>
        new(RealtimeMessagePersistKind.NotAllowed, messageId);
}

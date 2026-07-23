namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class RealtimeAttachmentRecord
{
    public required string AttachmentId { get; init; }
    public required long UploaderUserId { get; init; }
    public required string ObjectKey { get; init; }
    public string? PublicUrl { get; init; }
    public required string ContentType { get; init; }
    public required long SizeBytes { get; init; }
    public string? OriginalName { get; init; }
    public required AttachmentStatus Status { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? ClientAttachmentId { get; init; }
    public long CreatedAtMs { get; init; }
    public long? ConfirmedAtMs { get; init; }
    public long? BoundAtMs { get; init; }
}

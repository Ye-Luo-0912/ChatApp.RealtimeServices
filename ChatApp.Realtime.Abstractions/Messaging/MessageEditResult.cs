namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? Content { get; init; }
    public int? EditVersion { get; init; }
    public long? EditedAtMs { get; init; }

    public static MessageEditResult Success(
        string requestId,
        string messageId,
        string? conversationId,
        string content,
        int editVersion,
        long editedAtMs) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            MessageId = messageId,
            ConversationId = conversationId,
            Content = content,
            EditVersion = editVersion,
            EditedAtMs = editedAtMs
        };

    public static MessageEditResult Failed(
        string requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
}

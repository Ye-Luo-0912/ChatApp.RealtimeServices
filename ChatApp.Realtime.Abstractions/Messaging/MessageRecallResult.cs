namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageRecallResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long? RecalledAtMs { get; init; }

    public static MessageRecallResult Success(
        string requestId,
        string messageId,
        string? conversationId,
        long recalledAtMs) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            MessageId = messageId,
            ConversationId = conversationId,
            RecalledAtMs = recalledAtMs
        };

    public static MessageRecallResult Failed(
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

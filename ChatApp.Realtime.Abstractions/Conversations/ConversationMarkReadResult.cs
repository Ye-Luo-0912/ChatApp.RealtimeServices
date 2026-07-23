namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationMarkReadResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public int UnreadCount { get; init; }
    public string? LastReadMessageId { get; init; }
    public long? LastReadAtMs { get; init; }
    public bool Changed { get; init; }

    public static ConversationMarkReadResult Success(
        string requestId,
        string conversationId,
        int unreadCount,
        string? lastReadMessageId,
        long? lastReadAtMs,
        bool changed) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            UnreadCount = unreadCount,
            LastReadMessageId = lastReadMessageId,
            LastReadAtMs = lastReadAtMs,
            Changed = changed
        };

    public static ConversationMarkReadResult Failed(
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

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationSetPrefsResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public string? ConversationId { get; init; }
    public bool IsPinned { get; init; }
    public bool IsMuted { get; init; }
    public long? MutedUntilMs { get; init; }
    public bool Changed { get; init; }

    public static ConversationSetPrefsResult Success(
        string requestId,
        string conversationId,
        bool isPinned,
        bool isMuted,
        long? mutedUntilMs,
        bool changed) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            IsPinned = isPinned,
            IsMuted = isMuted,
            MutedUntilMs = mutedUntilMs,
            Changed = changed
        };

    public static ConversationSetPrefsResult Failed(
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

    public static ConversationSetPrefsResult ServerBusy(
        string requestId,
        int retryAfterMs,
        string queueKind) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = "server_busy",
            ErrorMessage = "服务繁忙，请稍后重试。",
            RetryAfterMs = retryAfterMs,
            QueueKind = queueKind
        };
}

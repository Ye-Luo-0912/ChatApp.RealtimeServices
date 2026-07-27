namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageReactionResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? Emoji { get; init; }
    public MessageReactionAction? Action { get; init; }
    public long? OccurredAtMs { get; init; }
    public int? EmojiCount { get; init; }

    public static MessageReactionResult Success(
        string requestId,
        string messageId,
        string? conversationId,
        string emoji,
        MessageReactionAction action,
        long occurredAtMs,
        int emojiCount) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            MessageId = messageId,
            ConversationId = conversationId,
            Emoji = emoji,
            Action = action,
            OccurredAtMs = occurredAtMs,
            EmojiCount = emojiCount
        };

    public static MessageReactionResult Failed(
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

    public static MessageReactionResult ServerBusy(
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

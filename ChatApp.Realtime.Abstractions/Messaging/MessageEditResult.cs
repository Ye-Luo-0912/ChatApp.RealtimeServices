namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? Content { get; init; }
    public int? EditVersion { get; init; }
    public long? EditedAtMs { get; init; }

    /// <summary>
    /// 一-4：本次 Edit 新增的 @提及用户 Id 列表。
    /// </summary>
    public IReadOnlyList<long>? AddedMentionedUserIds { get; init; }

    /// <summary>
    /// 一-4：本次 Edit 移除的 @提及用户 Id 列表。
    /// </summary>
    public IReadOnlyList<long>? RemovedMentionedUserIds { get; init; }

    public static MessageEditResult Success(
        string requestId,
        string messageId,
        string? conversationId,
        string content,
        int editVersion,
        long editedAtMs,
        IReadOnlyList<long>? addedMentionedUserIds = null,
        IReadOnlyList<long>? removedMentionedUserIds = null) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            MessageId = messageId,
            ConversationId = conversationId,
            Content = content,
            EditVersion = editVersion,
            EditedAtMs = editedAtMs,
            AddedMentionedUserIds = addedMentionedUserIds,
            RemovedMentionedUserIds = removedMentionedUserIds
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

    public static MessageEditResult ServerBusy(
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

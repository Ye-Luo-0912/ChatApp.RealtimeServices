namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationListPage
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public IReadOnlyList<ConversationListItem> Items { get; init; } = [];
    public ConversationListCursor? NextCursor { get; init; }
    public bool HasMore { get; init; }

    public static ConversationListPage Success(
        string requestId,
        IReadOnlyList<ConversationListItem> items,
        ConversationListCursor? nextCursor,
        bool hasMore) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            Items = items,
            NextCursor = nextCursor,
            HasMore = hasMore
        };

    public static ConversationListPage Failed(
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

    public static ConversationListPage ServerBusy(
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

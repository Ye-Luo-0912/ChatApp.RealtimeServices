namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class MessageHistoryPage
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public IReadOnlyList<RealtimeHistoryMessage> Items { get; init; } = [];
    public MessageHistoryCursor? NextCursor { get; init; }
    public bool HasMore { get; init; }

    public static MessageHistoryPage Success(
        string requestId,
        IReadOnlyList<RealtimeHistoryMessage> items,
        MessageHistoryCursor? nextCursor,
        bool hasMore) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            Items = items,
            NextCursor = nextCursor,
            HasMore = hasMore
        };

    public static MessageHistoryPage Failed(
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

    public static MessageHistoryPage ServerBusy(
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

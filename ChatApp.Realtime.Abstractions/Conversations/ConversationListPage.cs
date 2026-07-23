namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationListPage
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
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
}

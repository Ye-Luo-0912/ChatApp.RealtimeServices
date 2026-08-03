namespace ChatApp.Realtime.Abstractions.Relationships;

public sealed class RelationshipListResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public IReadOnlyList<RelationshipListItem>? Items { get; init; }
    public string? NextCursor { get; init; }
    public bool HasMore { get; init; }

    public static RelationshipListResult Success(
        string requestId,
        IReadOnlyList<RelationshipListItem> items,
        string? nextCursor = null,
        bool hasMore = false) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            Items = items,
            NextCursor = nextCursor,
            HasMore = hasMore
        };

    public static RelationshipListResult Failed(
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

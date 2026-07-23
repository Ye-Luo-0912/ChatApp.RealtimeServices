using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class ConversationHistoryCatchUp
{
    public required string ConversationId { get; init; }
    public IReadOnlyList<RealtimeHistoryMessage> Items { get; init; } = [];
    public bool HasMore { get; init; }
    public MessageHistoryCursor? NextCursor { get; init; }
}

public sealed class SyncBootstrapPage
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public long ServerTimeMs { get; init; }
    public IReadOnlyList<ConversationListItem> Conversations { get; init; } = [];
    public ConversationListCursor? ConversationsNextCursor { get; init; }
    public bool ConversationsHasMore { get; init; }
    public IReadOnlyList<ConversationHistoryCatchUp> CatchUps { get; init; } = [];

    public static SyncBootstrapPage Success(
        string requestId,
        long serverTimeMs,
        IReadOnlyList<ConversationListItem> conversations,
        ConversationListCursor? conversationsNextCursor,
        bool conversationsHasMore,
        IReadOnlyList<ConversationHistoryCatchUp> catchUps) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ServerTimeMs = serverTimeMs,
            Conversations = conversations,
            ConversationsNextCursor = conversationsNextCursor,
            ConversationsHasMore = conversationsHasMore,
            CatchUps = catchUps
        };

    public static SyncBootstrapPage Failed(
        string requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            ServerTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
}

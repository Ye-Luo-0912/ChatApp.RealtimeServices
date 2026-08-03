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
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public long ServerTimeMs { get; init; }
    public IReadOnlyList<ConversationListItem> Conversations { get; init; } = [];
    public ConversationListCursor? ConversationsNextCursor { get; init; }
    public bool ConversationsHasMore { get; init; }
    public IReadOnlyList<ConversationHistoryCatchUp> CatchUps { get; init; } = [];

    /// <summary>
    /// Conversations whose client/device cursors are invalid; client must wipe local cache and full-resync.
    /// Additive: absent/empty means no resets (happy path).
    /// </summary>
    public IReadOnlyList<SyncCursorResetRequired> ResetsRequired { get; init; } = [];

    /// <summary>
    /// 关系列表增量同步结果。null 或空表示未请求关系同步。
    /// </summary>
    public IReadOnlyList<RelationshipCatchUp>? RelationshipCatchUps { get; init; }

    public static SyncBootstrapPage Success(
        string requestId,
        long serverTimeMs,
        IReadOnlyList<ConversationListItem> conversations,
        ConversationListCursor? conversationsNextCursor,
        bool conversationsHasMore,
        IReadOnlyList<ConversationHistoryCatchUp> catchUps,
        IReadOnlyList<SyncCursorResetRequired>? resetsRequired = null,
        IReadOnlyList<RelationshipCatchUp>? relationshipCatchUps = null) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ServerTimeMs = serverTimeMs,
            Conversations = conversations,
            ConversationsNextCursor = conversationsNextCursor,
            ConversationsHasMore = conversationsHasMore,
            CatchUps = catchUps,
            ResetsRequired = resetsRequired ?? [],
            RelationshipCatchUps = relationshipCatchUps
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

    public static SyncBootstrapPage ServerBusy(
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
            QueueKind = queueKind,
            ServerTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
}
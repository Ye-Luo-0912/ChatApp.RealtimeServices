using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class GroupConversationResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public string? ConversationId { get; init; }
    public string? Title { get; init; }
    public ConversationType Type { get; init; } = ConversationType.Group;
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }

    /// <summary>P1-2：QueryAudience 结果——会话受众版本号（每次成员变更 +1）。</summary>
    public long AudienceVersion { get; init; }

    /// <summary>P1-2：QueryAudience 结果——当前活跃成员用户编号（升序）。</summary>
    public IReadOnlyList<long>? AudienceMemberUserIds { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——已读人数。</summary>
    public int ReadCount { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——总成员人数（不含已离群成员）。</summary>
    public int TotalMemberCount { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——是否为小群（返回完整 list 而非仅 count）。</summary>
    public bool IsSmallGroup { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——已读者列表（小群）。</summary>
    public IReadOnlyList<MessageReader>? Readers { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——下一页游标。</summary>
    public long? NextCursor { get; init; }

    /// <summary>P1-4：QueryReadReceipts 结果——是否还有下一页。</summary>
    public bool HasMore { get; init; }

    public static GroupConversationResult Success(
        string requestId,
        string conversationId,
        string? title = null,
        IReadOnlyList<ConversationMemberItem>? members = null) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            Title = title,
            Members = members
        };

    /// <summary>P1-2：QueryAudience 成功结果。</summary>
    public static GroupConversationResult SuccessAudience(
        string requestId,
        string conversationId,
        long audienceVersion,
        IReadOnlyList<long> memberUserIds) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            AudienceVersion = audienceVersion,
            AudienceMemberUserIds = memberUserIds
        };

    /// <summary>P1-4：QueryReadReceipts 成功结果。</summary>
    public static GroupConversationResult SuccessReadReceipt(
        string requestId,
        string conversationId,
        int readCount,
        int totalMemberCount,
        bool isSmallGroup,
        IReadOnlyList<MessageReader>? readers = null,
        long? nextCursor = null,
        bool hasMore = false) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            ReadCount = readCount,
            TotalMemberCount = totalMemberCount,
            IsSmallGroup = isSmallGroup,
            Readers = readers,
            NextCursor = nextCursor,
            HasMore = hasMore
        };

    public static GroupConversationResult Failed(
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

    public static GroupConversationResult ServerBusy(
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
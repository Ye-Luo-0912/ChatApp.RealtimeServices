namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// P1-2：会话受众查询结果。
/// <para>
/// <see cref="AudienceVersion"/> 为会话成员集合的版本号（每次成员变更 +1）。
/// <see cref="MemberUserIds"/> 为当前活跃成员用户编号（升序）。
/// Gateway 据此校验本地 ConversationAudienceCache 是否过期；版本落后时重建缓存。
/// </para>
/// </summary>
public sealed class ConversationAudienceResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public long AudienceVersion { get; init; }
    public long[]? MemberUserIds { get; init; }

    public static ConversationAudienceResult Success(
        string requestId,
        string conversationId,
        long audienceVersion,
        long[] memberUserIds) => new()
    {
        RequestId = requestId,
        Succeeded = true,
        ConversationId = conversationId,
        AudienceVersion = audienceVersion,
        MemberUserIds = memberUserIds
    };

    public static ConversationAudienceResult Failed(
        string requestId,
        string errorCode,
        string errorMessage) => new()
    {
        RequestId = requestId,
        Succeeded = false,
        ErrorCode = errorCode,
        ErrorMessage = errorMessage
    };
}
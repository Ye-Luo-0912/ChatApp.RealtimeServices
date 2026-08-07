namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// P1-2：会话受众查询请求（Gateway NATS request/reply）。
/// 由 Gateway 的 <c>ConversationAudienceCache</c> 在冷启动或
/// AudienceVersion 落后时作为 request/reply 请求载荷发起。
/// </summary>
public sealed class ConversationAudienceQuery
{
    public required string RequestId { get; init; }
    public required string ConversationId { get; init; }

    /// <summary>上行会话 Id（用于 NATS 身份头注入，非鉴权依据）。</summary>
    public string? ActorSessionId { get; init; }
}

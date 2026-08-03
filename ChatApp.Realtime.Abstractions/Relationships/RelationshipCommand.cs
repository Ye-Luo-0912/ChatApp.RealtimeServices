namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// 关系变更命令（Core NATS request/reply）。
/// </summary>
public sealed class RelationshipCommand
{
    public required string RequestId { get; init; }
    public long ActorUserId { get; init; }
    public RelationshipOperation Operation { get; init; }

    /// <summary>目标用户 Id（对方）。</summary>
    public long? TargetUserId { get; init; }

    /// <summary>好友请求附言（仅 SendFriendRequest 时使用）。</summary>
    public string? Message { get; init; }

    /// <summary>好友请求 Id（仅 RespondFriendRequest 时使用：接受或拒绝指定请求）。</summary>
    public string? RequestIdToRespond { get; init; }

    /// <summary>上行会话 Id（幂等 / 回声跳过）。</summary>
    public string? ActorSessionId { get; init; }
}

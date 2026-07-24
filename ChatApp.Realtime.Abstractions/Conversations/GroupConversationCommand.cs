namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 群会话变更 / 成员查询命令（Core NATS request/reply）。
/// </summary>
public sealed class GroupConversationCommand
{
    public required string RequestId { get; init; }
    public long ActorUserId { get; init; }
    public GroupConversationOperation Operation { get; init; }

    /// <summary>已有群会话 Id；Create 时可空（服务端生成）。</summary>
    public string? ConversationId { get; init; }

    /// <summary>Create：群标题（必填）。</summary>
    public string? Title { get; init; }

    /// <summary>Create / AddMembers：待加入用户 Id。</summary>
    public IReadOnlyList<long>? MemberUserIds { get; init; }

    /// <summary>RemoveMember / ChangeRole：目标用户。</summary>
    public long? TargetUserId { get; init; }

    /// <summary>ChangeRole：目标角色（Owner 表示转让所有权）。</summary>
    public ConversationMemberRole? NewRole { get; init; }

    /// <summary>上行会话 Id（幂等 / 回声跳过）。</summary>
    public string? ActorSessionId { get; init; }
}

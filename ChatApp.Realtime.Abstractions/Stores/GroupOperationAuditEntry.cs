using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 群操作审计记录。统一记录所有群管理操作的关键信息。
/// </summary>
public sealed record GroupOperationAuditEntry
{
    /// <summary>操作者用户编号。</summary>
    public required long ActorUserId { get; init; }

    /// <summary>群会话编号。Create 操作在成功后才有值。</summary>
    public string? ConversationId { get; init; }

    /// <summary>群管理操作类型。</summary>
    public required GroupConversationOperation Operation { get; init; }

    /// <summary>目标用户编号（RemoveMember / ChangeRole 使用）。</summary>
    public long? TargetUserId { get; init; }

    /// <summary>变更前角色（仅 ChangeRole 有值）。</summary>
    public ConversationMemberRole? PreviousRole { get; init; }

    /// <summary>变更后角色（仅 ChangeRole 有值）。</summary>
    public ConversationMemberRole? NewRole { get; init; }

    /// <summary>客户端请求编号（幂等键）。</summary>
    public required string RequestId { get; init; }

    /// <summary>网关会话编号。</summary>
    public string? ActorSessionId { get; init; }

    /// <summary>操作是否成功。</summary>
    public required bool Succeeded { get; init; }

    /// <summary>失败错误码（成功时为 null）。</summary>
    public string? ErrorCode { get; init; }

    /// <summary>操作时间戳（毫秒）。</summary>
    public required long OccurredAtMs { get; init; }
}

/// <summary>
/// 群操作审计存储。记录所有群管理操作的审计轨迹。
/// 审计写入为 best-effort，不阻断主流程。
/// </summary>
public interface IGroupOperationAuditStore
{
    /// <summary>记录一条群操作审计。best-effort，不抛出异常。</summary>
    Task RecordAsync(GroupOperationAuditEntry entry, CancellationToken ct = default);
}

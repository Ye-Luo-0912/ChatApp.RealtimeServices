using System.Data.Common;
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
/// <para>
/// 提供两条写入路径：
/// <list type="bullet">
/// <item><see cref="RecordAsync"/>：事务外 best-effort 写入（失败尝试审计），
/// 异常被捕获并记录，不阻断主流程。</item>
/// <item><see cref="RecordInTransactionAsync"/>：业务事务内写入（审计 Outbox），
/// 复用调用方连接与事务；审计失败向上抛出，导致整个业务事务回滚，
/// 保证“业务变更成功 ⇒ 审计已记录”的原子性，审计不会静默丢失。</item>
/// </list>
/// </para>
/// </summary>
public interface IGroupOperationAuditStore
{
    /// <summary>
    /// 记录一条群操作审计（事务外，best-effort）。异常被捕获并记录，不阻断主流程。
    /// 用于失败尝试审计：业务事务已回滚，无法在事务内记录，仅作 best-effort 留痕。
    /// </summary>
    Task RecordAsync(GroupOperationAuditEntry entry, CancellationToken ct = default);

    /// <summary>
    /// 在业务事务内记录审计（审计 Outbox）。复用调用方已有的连接与事务，
    /// 消除事务外独立连接获取。
    /// <para>
    /// 与 <see cref="RecordAsync"/> 不同：事务内失败不再被吞掉——审计异常向上抛出，
    /// 让整个业务事务回滚，保证审计记录与业务变更同生共死（审计 Outbox 语义）。
    /// </para>
    /// </summary>
    Task RecordInTransactionAsync(
        GroupOperationAuditEntry entry,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct = default);
}

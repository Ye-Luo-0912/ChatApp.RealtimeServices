using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messaging;

/// <summary>
/// 消息变更（撤回 / 编辑 / Reaction）的统一权限策略。
/// <para>
/// P0-8：原发送者在被移出群后仍可编辑、撤回或对其旧消息添加 Reaction，
/// 因为旧实现仅检查 <c>target.SenderUserId == actorUserId</c> 或 participant 关系，
/// 未再次校验当前群成员资格。本策略在同一事务内查询 <c>conversation_members</c>，
/// 确保操作者仍是活跃成员（群未解散、未被移除、未主动退群）。
/// </para>
/// <para>
/// 单聊（<see cref="MessageMutationContext.ConversationId"/> 为空或非群 ID）不查成员表，
/// 仅校验 participant 关系。
/// </para>
/// </summary>
public interface IConversationMessageMutationPolicy
{
    /// <summary>
    /// 在当前事务内校验消息变更权限。
    /// </summary>
    /// <param name="connection">事务连接。</param>
    /// <param name="transaction">事务句柄（可为 null，表示隐式事务）。</param>
    /// <param name="schema">数据库 schema 元数据。</param>
    /// <param name="context">变更上下文：会话、原发送者/接收者、操作者、操作类型。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>授权结果；<see cref="MessageMutationAuthorization.Allowed"/> 为 true 时放行。</returns>
    Task<MessageMutationAuthorization> AuthorizeMutationAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        MessageMutationContext context,
        CancellationToken ct);
}

/// <summary>消息变更操作类型。不同类型对操作者身份有不同要求。</summary>
public enum MessageMutationOperation
{
    /// <summary>撤回：要求操作者既是原发送者，又是当前群成员。</summary>
    Recall,

    /// <summary>编辑：要求操作者既是原发送者，又是当前群成员。</summary>
    Edit,

    /// <summary>Reaction：要求操作者是当前群成员（不要求是发送者）。</summary>
    Reaction
}

/// <summary>消息变更权限校验上下文。</summary>
public readonly record struct MessageMutationContext(
    string? ConversationId,
    long MessageSenderId,
    long MessageReceiverUserId,
    long ActorUserId,
    MessageMutationOperation Operation);

/// <summary>权限校验结果。</summary>
public readonly record struct MessageMutationAuthorization(
    bool Allowed,
    string? ErrorCode,
    string? Reason)
{
    public static MessageMutationAuthorization Allow() => new(true, null, null);

    public static MessageMutationAuthorization Deny(string errorCode, string reason) =>
        new(false, errorCode, reason);
}

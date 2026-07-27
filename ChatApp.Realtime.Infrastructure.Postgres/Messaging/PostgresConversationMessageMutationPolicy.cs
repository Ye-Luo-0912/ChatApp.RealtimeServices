using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messaging;

/// <summary>
/// <see cref="IConversationMessageMutationPolicy"/> 的 Npgsql 实现。
/// <para>
/// 单聊（conversation_id 为空或 <see cref="ConversationId.IsDirect"/>）：
/// <list type="bullet">
/// <item>Recall / Edit：操作者必须是原发送者。</item>
/// <item>Reaction：操作者必须是发送者或接收者。</item>
/// </list>
/// </para>
/// <para>
/// 群聊（<see cref="ConversationId.IsGroup"/>）：
/// <list type="bullet">
/// <item>所有操作都要求操作者仍是 <c>conversation_members</c> 中的活跃成员。</item>
/// <item>Recall / Edit 还要求操作者是原发送者。</item>
/// </list>
/// 群不存在或已解散（conversation 行缺失）时，成员查询自然返回空，操作被拒绝。
/// </para>
/// <para>
/// 所有查询都在调用方事务内执行（FOR UPDATE 无需，因 conversation_members 行级一致性），
/// 保证校验与业务变更原子可见。
/// </para>
/// </summary>
public sealed class PostgresConversationMessageMutationPolicy : IConversationMessageMutationPolicy
{
    private readonly ILogger<PostgresConversationMessageMutationPolicy> _logger;

    public PostgresConversationMessageMutationPolicy(
        ILogger<PostgresConversationMessageMutationPolicy> logger)
    {
        _logger = logger;
    }

    public async Task<MessageMutationAuthorization> AuthorizeMutationAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        MessageMutationContext context,
        CancellationToken ct)
    {
        var conversationId = context.ConversationId;
        var isGroup = !string.IsNullOrWhiteSpace(conversationId)
                      && ConversationId.IsGroup(conversationId);

        if (!isGroup)
        {
            return AuthorizeDirect(context);
        }

        // 群聊：必须仍是活跃成员。
        var isActiveMember = await IsActiveMemberAsync(
                connection,
                transaction,
                schema,
                conversationId!,
                context.ActorUserId,
                ct)
            .ConfigureAwait(false);

        if (!isActiveMember)
        {
            _logger.LogInformation(
                "群消息变更被拒绝：操作者不是当前群成员。会话={ConversationId}；操作者={ActorUserId}；操作={Operation}",
                conversationId,
                context.ActorUserId,
                context.Operation);
            return MessageMutationAuthorization.Deny(
                "not_group_member",
                "操作者已不是该群成员，无法修改群消息。");
        }

        // Recall / Edit 还要求是原发送者。
        if (context.Operation is MessageMutationOperation.Recall
            or MessageMutationOperation.Edit
            && context.MessageSenderId != context.ActorUserId)
        {
            _logger.LogInformation(
                "群消息变更被拒绝：操作者不是原发送者。会话={ConversationId}；操作者={ActorUserId}；原发送者={SenderUserId}；操作={Operation}",
                conversationId,
                context.ActorUserId,
                context.MessageSenderId,
                context.Operation);
            return MessageMutationAuthorization.Deny(
                "not_sender",
                "仅原发送者可撤回或编辑消息。");
        }

        return MessageMutationAuthorization.Allow();
    }

    private static MessageMutationAuthorization AuthorizeDirect(MessageMutationContext context)
    {
        var isSender = context.ActorUserId == context.MessageSenderId;
        var isReceiver = context.ActorUserId == context.MessageReceiverUserId;

        switch (context.Operation)
        {
            case MessageMutationOperation.Recall:
            case MessageMutationOperation.Edit:
                if (!isSender)
                    return MessageMutationAuthorization.Deny(
                        "not_sender",
                        "仅原发送者可撤回或编辑消息。");
                return MessageMutationAuthorization.Allow();

            case MessageMutationOperation.Reaction:
                if (!isSender && !isReceiver)
                    return MessageMutationAuthorization.Deny(
                        "not_participant",
                        "仅消息发送者或接收者可添加 Reaction。");
                return MessageMutationAuthorization.Allow();

            default:
                return MessageMutationAuthorization.Deny(
                    "invalid_operation",
                    "不支持的消息变更操作。");
        }
    }

    private static async Task<bool> IsActiveMemberAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long userId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {schema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
               AND user_id = @user_id
               AND left_at_ms IS NULL
             LIMIT 1;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is not null;
    }
}

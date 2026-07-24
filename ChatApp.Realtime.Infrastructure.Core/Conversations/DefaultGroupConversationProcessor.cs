using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class DefaultGroupConversationProcessor : IGroupConversationProcessor
{
    private readonly IRealtimeGroupStore _store;
    private readonly IRealtimeOutboxSignal _outboxSignal;

    public DefaultGroupConversationProcessor(
        IRealtimeGroupStore store,
        IRealtimeOutboxSignal outboxSignal)
    {
        _store = store;
        _outboxSignal = outboxSignal;
    }

    public async Task<GroupConversationResult> ProcessAsync(
        GroupConversationCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        var occurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        switch (command.Operation)
        {
            case GroupConversationOperation.Create:
            {
                var conversationId = ConversationId.CreateGroup();
                var title = command.Title!.Trim();
                var result = await _store.CreateGroupAsync(
                        command.ActorUserId,
                        conversationId,
                        title,
                        command.MemberUserIds ?? Array.Empty<long>(),
                        command.ActorSessionId,
                        occurredAtMs,
                        ct)
                    .ConfigureAwait(false);
                if (!result.Succeeded)
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        result.ErrorCode!,
                        result.ErrorMessage!);
                _outboxSignal.Notify();
                return GroupConversationResult.Success(
                    command.RequestId,
                    result.ConversationId!,
                    result.Title,
                    result.Members);
            }
            case GroupConversationOperation.AddMembers:
            {
                var result = await _store.AddMembersAsync(
                        command.ActorUserId,
                        command.ConversationId!.Trim(),
                        command.MemberUserIds ?? Array.Empty<long>(),
                        command.ActorSessionId,
                        occurredAtMs,
                        ct)
                    .ConfigureAwait(false);
                if (!result.Succeeded)
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        result.ErrorCode!,
                        result.ErrorMessage!);
                if (result.Members is { Count: > 0 })
                    _outboxSignal.Notify();
                return GroupConversationResult.Success(
                    command.RequestId,
                    result.ConversationId!,
                    result.Title,
                    result.Members);
            }
            case GroupConversationOperation.RemoveMember:
            {
                var result = await _store.RemoveMemberAsync(
                        command.ActorUserId,
                        command.ConversationId!.Trim(),
                        command.TargetUserId!.Value,
                        command.ActorSessionId,
                        occurredAtMs,
                        ct)
                    .ConfigureAwait(false);
                if (!result.Succeeded)
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        result.ErrorCode!,
                        result.ErrorMessage!);
                _outboxSignal.Notify();
                return GroupConversationResult.Success(
                    command.RequestId,
                    result.ConversationId!,
                    result.Title);
            }
            case GroupConversationOperation.Leave:
            {
                var result = await _store.LeaveAsync(
                        command.ActorUserId,
                        command.ConversationId!.Trim(),
                        command.ActorSessionId,
                        occurredAtMs,
                        ct)
                    .ConfigureAwait(false);
                if (!result.Succeeded)
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        result.ErrorCode!,
                        result.ErrorMessage!);
                _outboxSignal.Notify();
                return GroupConversationResult.Success(
                    command.RequestId,
                    result.ConversationId!,
                    result.Title);
            }
            case GroupConversationOperation.ChangeRole:
            {
                var result = await _store.ChangeRoleAsync(
                        command.ActorUserId,
                        command.ConversationId!.Trim(),
                        command.TargetUserId!.Value,
                        command.NewRole!.Value,
                        command.ActorSessionId,
                        occurredAtMs,
                        ct)
                    .ConfigureAwait(false);
                if (!result.Succeeded)
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        result.ErrorCode!,
                        result.ErrorMessage!);
                _outboxSignal.Notify();
                return GroupConversationResult.Success(
                    command.RequestId,
                    result.ConversationId!,
                    result.Title);
            }
            case GroupConversationOperation.ListMembers:
            {
                var members = await _store.ListMembersAsync(
                        command.ActorUserId,
                        command.ConversationId!.Trim(),
                        ct)
                    .ConfigureAwait(false);
                if (members.Count == 0)
                {
                    var isMember = await _store.IsActiveMemberAsync(
                            command.ConversationId.Trim(),
                            command.ActorUserId,
                            ct)
                        .ConfigureAwait(false);
                    if (!isMember)
                    {
                        return GroupConversationResult.Failed(
                            command.RequestId,
                            "forbidden",
                            "无权查看该群成员。");
                    }
                }

                return GroupConversationResult.Success(
                    command.RequestId,
                    command.ConversationId.Trim(),
                    members: members);
            }
            default:
                return GroupConversationResult.Failed(
                    command.RequestId,
                    "invalid_operation",
                    "不支持的群操作。");
        }
    }

    private static GroupConversationResult? Validate(GroupConversationCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return GroupConversationResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (command.ActorUserId <= 0)
            return GroupConversationResult.Failed(
                command.RequestId,
                "invalid_user_id",
                "操作用户编号必须大于 0。");

        switch (command.Operation)
        {
            case GroupConversationOperation.Create:
                if (string.IsNullOrWhiteSpace(command.Title)
                    || command.Title.Trim().Length > ConversationId.TitleMaxLength)
                {
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        "invalid_title",
                        $"群标题不能为空且长度不能超过 {ConversationId.TitleMaxLength}。");
                }

                break;
            case GroupConversationOperation.AddMembers:
            case GroupConversationOperation.RemoveMember:
            case GroupConversationOperation.Leave:
            case GroupConversationOperation.ChangeRole:
            case GroupConversationOperation.ListMembers:
                if (string.IsNullOrWhiteSpace(command.ConversationId)
                    || !ConversationId.IsGroup(command.ConversationId.Trim()))
                {
                    return GroupConversationResult.Failed(
                        command.RequestId,
                        "invalid_conversation_id",
                        "群会话编号无效。");
                }

                break;
            default:
                return GroupConversationResult.Failed(
                    command.RequestId,
                    "invalid_operation",
                    "不支持的群操作。");
        }

        if (command.Operation is GroupConversationOperation.RemoveMember
            or GroupConversationOperation.ChangeRole)
        {
            if (command.TargetUserId is null or <= 0)
            {
                return GroupConversationResult.Failed(
                    command.RequestId,
                    "invalid_target",
                    "目标用户编号无效。");
            }
        }

        if (command.Operation == GroupConversationOperation.ChangeRole
            && command.NewRole is null)
        {
            return GroupConversationResult.Failed(
                command.RequestId,
                "invalid_role",
                "必须指定新角色。");
        }

        if (command.Operation == GroupConversationOperation.AddMembers
            && (command.MemberUserIds is null || command.MemberUserIds.Count == 0))
        {
            return GroupConversationResult.Failed(
                command.RequestId,
                "invalid_members",
                "至少需要一名有效成员。");
        }

        return null;
    }
}

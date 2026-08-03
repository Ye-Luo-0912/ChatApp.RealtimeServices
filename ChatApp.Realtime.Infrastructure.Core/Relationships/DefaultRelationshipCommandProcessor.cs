using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

/// <summary>
/// 关系变更命令处理器：校验 → 调用 <see cref="IRelationshipStore"/> → 映射结果。
/// <para>遵循 <see cref="DefaultGroupConversationProcessor"/> 模式。</para>
/// </summary>
public sealed class DefaultRelationshipCommandProcessor : IRelationshipCommandProcessor
{
    private readonly IRelationshipStore _store;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;

    public DefaultRelationshipCommandProcessor(
        IRelationshipStore store,
        IUserDeletionTombstoneStore tombstoneStore)
    {
        _store = store;
        _tombstoneStore = tombstoneStore;
    }

    public async Task<RelationshipCommandResult> ProcessAsync(
        RelationshipCommand command, CancellationToken ct = default)
    {
        if (await _tombstoneStore.IsUserDeletedAsync(command.ActorUserId, ct).ConfigureAwait(false))
        {
            return RelationshipCommandResult.Failed(
                command.RequestId, "user_deleted", "用户已注销，操作被拒绝。");
        }

        var occurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        RelationshipMutatePersistResult result;
        switch (command.Operation)
        {
            case RelationshipOperation.SendFriendRequest:
                result = await _store.SendFriendRequestAsync(
                    command.RequestId, command.ActorUserId, command.TargetUserId!.Value,
                    command.Message, command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            case RelationshipOperation.AcceptFriendRequest:
                result = await _store.AcceptFriendRequestAsync(
                    command.RequestId, command.ActorUserId, command.RequestIdToRespond!,
                    command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            case RelationshipOperation.DeclineFriendRequest:
                result = await _store.DeclineFriendRequestAsync(
                    command.RequestId, command.ActorUserId, command.RequestIdToRespond!,
                    command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            case RelationshipOperation.RemoveFriend:
                result = await _store.RemoveFriendAsync(
                    command.RequestId, command.ActorUserId, command.TargetUserId!.Value,
                    command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            case RelationshipOperation.BlockUser:
                result = await _store.BlockUserAsync(
                    command.RequestId, command.ActorUserId, command.TargetUserId!.Value,
                    command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            case RelationshipOperation.UnblockUser:
                result = await _store.UnblockUserAsync(
                    command.RequestId, command.ActorUserId, command.TargetUserId!.Value,
                    command.ActorSessionId, occurredAtMs, ct).ConfigureAwait(false);
                break;
            default:
                return RelationshipCommandResult.Failed(
                    command.RequestId, "unknown_operation", "未知关系操作类型。");
        }

        if (!result.Succeeded)
            return RelationshipCommandResult.Failed(
                command.RequestId, result.ErrorCode!, result.ErrorMessage!);

        return RelationshipCommandResult.Success(
            command.RequestId, command.Operation, result.TargetUserId, result.ResourceId);
    }
}
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeGroupStore : IRealtimeGroupStore
{
    public Task<GroupCreatePersistResult> CreateGroupAsync(
        long creatorUserId,
        string conversationId,
        string title,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<GroupMutatePersistResult> AddMembersAsync(
        long actorUserId,
        string conversationId,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<GroupMutatePersistResult> RemoveMemberAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<GroupMutatePersistResult> LeaveAsync(
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<GroupMutatePersistResult> ChangeRoleAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        ConversationMemberRole newRole,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
        long actorUserId,
        string conversationId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实群会话存储。");
    }

    public Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
        string conversationId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<long>>(Array.Empty<long>());
    }

    public Task<bool> IsActiveMemberAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(false);
    }
}

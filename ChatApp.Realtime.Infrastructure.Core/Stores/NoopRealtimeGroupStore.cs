using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeGroupStore : IRealtimeGroupStore
{
    public Task<GroupCreatePersistResult> CreateGroupAsync(
        string requestId,
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
        string requestId,
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
        string requestId,
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
        string requestId,
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
        string requestId,
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

    public Task<GroupMutatePersistResult> DissolveAsync(
        string requestId,
        long actorUserId,
        string conversationId,
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

    public Task<ConversationMemberRole?> GetMemberRoleAsync(
        long userId,
        string conversationId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<ConversationMemberRole?>(null);
    }

    public Task<IReadOnlyList<long>> ValidateMembersAsync(
        string conversationId,
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<long>>(Array.Empty<long>());
    }

    public Task<ConversationAudienceLoadResult> QueryAudienceAsync(
        string conversationId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(ConversationAudienceLoadResult.Ok(0, Array.Empty<long>()));
    }
}
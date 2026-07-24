using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>群会话创建与成员变更（Realtime 权威）。</summary>
public interface IRealtimeGroupStore
{
    Task<GroupCreatePersistResult> CreateGroupAsync(
        long creatorUserId,
        string conversationId,
        string title,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> AddMembersAsync(
        long actorUserId,
        string conversationId,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> RemoveMemberAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> LeaveAsync(
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> ChangeRoleAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        ConversationMemberRole newRole,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
        long actorUserId,
        string conversationId,
        CancellationToken ct = default);

    Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
        string conversationId,
        CancellationToken ct = default);

    Task<bool> IsActiveMemberAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default);
}

public readonly record struct GroupCreatePersistResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    string? ConversationId,
    string? Title,
    IReadOnlyList<ConversationMemberItem>? Members)
{
    public static GroupCreatePersistResult Ok(
        string conversationId,
        string title,
        IReadOnlyList<ConversationMemberItem> members) =>
        new(true, null, null, conversationId, title, members);

    public static GroupCreatePersistResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null, null, null);
}

public readonly record struct GroupMutatePersistResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    string? ConversationId,
    string? Title,
    IReadOnlyList<ConversationMemberItem>? Members)
{
    public static GroupMutatePersistResult Ok(
        string conversationId,
        string? title = null,
        IReadOnlyList<ConversationMemberItem>? members = null) =>
        new(true, null, null, conversationId, title, members);

    public static GroupMutatePersistResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null, null, null);
}

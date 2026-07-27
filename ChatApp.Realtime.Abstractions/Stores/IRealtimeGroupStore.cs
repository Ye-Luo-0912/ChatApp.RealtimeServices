using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>群会话创建与成员变更（Realtime 权威）。</summary>
public interface IRealtimeGroupStore
{
    Task<GroupCreatePersistResult> CreateGroupAsync(
        string requestId,
        long creatorUserId,
        string conversationId,
        string title,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> AddMembersAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> RemoveMemberAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        long targetUserId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> LeaveAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<GroupMutatePersistResult> ChangeRoleAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        long targetUserId,
        ConversationMemberRole newRole,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default);

    /// <summary>解散群（仅 Owner）：全员标记 left_at_ms，会话标记 dissolved_at_ms，历史保留只读。</summary>
    Task<GroupMutatePersistResult> DissolveAsync(
        string requestId,
        long actorUserId,
        string conversationId,
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
        string? title = null,
        IReadOnlyList<ConversationMemberItem>? members = null) =>
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
    /// <summary>ChangeRole 操作的变更前角色（其他操作为 null）。</summary>
    public ConversationMemberRole? PreviousRole { get; init; }

    /// <summary>ChangeRole 操作的变更后角色（其他操作为 null）。</summary>
    public ConversationMemberRole? NewRole { get; init; }

    public static GroupMutatePersistResult Ok(
        string conversationId,
        string? title = null,
        IReadOnlyList<ConversationMemberItem>? members = null) =>
        new(true, null, null, conversationId, title, members);

    public static GroupMutatePersistResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null, null, null);
}

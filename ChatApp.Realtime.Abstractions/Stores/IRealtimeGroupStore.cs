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

    /// <summary>
    /// P0-3：查询指定用户在群会话中的角色（仅活跃成员）。
    /// 用于群消息 mention 规范化时判定发送者是否为管理员（Owner/Admin），
    /// 替代遍历全量成员列表。返回 null 表示用户不是活跃成员。
    /// </summary>
    Task<ConversationMemberRole?> GetMemberRoleAsync(
        long userId,
        string conversationId,
        CancellationToken ct = default);

    /// <summary>
    /// P0-3：批量校验指定用户集合中哪些是群的活跃成员。
    /// 用于群消息 mention 规范化时过滤非成员 mention，替代加载全量成员列表。
    /// 返回输入集合中属于活跃成员的子集（按 user_id 升序）。
    /// </summary>
    Task<IReadOnlyList<long>> ValidateMembersAsync(
        string conversationId,
        IReadOnlyList<long> userIds,
        CancellationToken ct = default);

    /// <summary>
    /// P1-2：查询会话受众（成员用户编号 + audience_version）。
    /// 与 <see cref="ListMembersAsync"/> 不同，本操作不要求调用者必须是活跃成员，
    /// 面向 Gateway 的会话级广播投递（AudienceKind=Conversation）解析成员集合。
    /// 返回空用户列表表示会话不存在或已解散。
    /// </summary>
    Task<ConversationAudienceLoadResult> QueryAudienceAsync(
        string conversationId,
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

public readonly record struct ConversationAudienceLoadResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    long AudienceVersion,
    IReadOnlyList<long> MemberUserIds)
{
    public static ConversationAudienceLoadResult Ok(
        long audienceVersion,
        IReadOnlyList<long> memberUserIds) =>
        new(true, null, null, audienceVersion, memberUserIds);

    public static ConversationAudienceLoadResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, 0, Array.Empty<long>());
}
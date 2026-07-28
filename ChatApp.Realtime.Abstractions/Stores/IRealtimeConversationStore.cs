using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeConversationStore
{
    Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default);

    /// <summary>
    /// 查询已离群（left_at_ms IS NOT NULL）的群会话列表，供用户查看历史。
    /// 单聊会话没有 left_at_ms 概念，此方法仅返回群会话（c.type = 2）。
    /// </summary>
    Task<IReadOnlyList<ConversationListItem>> QueryArchivedListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default);

    /// <summary>
    /// 将已读游标推进到指定位置（仅前进）。
    /// <paramref name="readMessageId"/> 为空时推进到会话当前最后消息；
    /// 非空时以库内该消息的 received_at_ms 为准（忽略 <paramref name="readAtMs"/>），并钳到 tip。
    /// 变更时同事务写入 UnreadCountChanged（读者）与 ConversationRead（其他活跃成员）Outbox。
    /// </summary>
    Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
        long userId,
        string conversationId,
        long? readAtMs,
        string? readMessageId,
        CancellationToken ct = default);

    /// <summary>
    /// 更新成员置顶 / 免打扰偏好。参数为 null 表示不修改该字段。
    /// 变更时同事务写入 ConversationListChanged Outbox。
    /// </summary>
    Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
        long userId,
        string conversationId,
        bool? pinned,
        bool? muted,
        long? mutedUntilMs,
        CancellationToken ct = default);
}

public readonly record struct ConversationReadAdvanceResult(
    bool Found,
    bool Changed,
    int UnreadCount,
    string? LastReadMessageId,
    long? LastReadAtMs);

public readonly record struct ConversationMemberPrefsResult(
    bool Found,
    bool Changed,
    bool IsPinned,
    bool IsMuted,
    long? MutedUntilMs);

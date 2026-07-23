namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 会话列表 keyset 游标：必须与排序字段一致
/// （置顶 → 置顶时间 → 最后消息时间 → ConversationId）。
/// </summary>
public sealed record ConversationListCursor(
    bool IsPinned,
    long? PinnedAtMs,
    long? LastMessageAtMs,
    string ConversationId);

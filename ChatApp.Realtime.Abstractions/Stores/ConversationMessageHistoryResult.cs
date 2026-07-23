using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 按会话查询历史的结果：成员校验与消息选择在同一 SQL 中完成，避免 TOCTOU。
/// </summary>
public readonly record struct ConversationMessageHistoryResult(
    bool IsMember,
    IReadOnlyList<RealtimeHistoryMessage> Messages)
{
    public static ConversationMessageHistoryResult Forbidden { get; } =
        new(false, Array.Empty<RealtimeHistoryMessage>());

    public static ConversationMessageHistoryResult Ok(
        IReadOnlyList<RealtimeHistoryMessage> messages) =>
        new(true, messages);
}

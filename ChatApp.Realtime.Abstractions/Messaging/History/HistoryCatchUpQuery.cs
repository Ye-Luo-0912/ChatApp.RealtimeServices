namespace ChatApp.Realtime.Abstractions.Messaging.History;

/// <summary>
/// 同步 catch-up 批量查询规格：After* 为空时按会话最新消息向前取。
/// </summary>
public sealed class HistoryCatchUpQuery
{
    public required string ConversationId { get; init; }
    public long? AfterReceivedAtMs { get; init; }
    public string? AfterMessageId { get; init; }
    public int Take { get; init; }
}

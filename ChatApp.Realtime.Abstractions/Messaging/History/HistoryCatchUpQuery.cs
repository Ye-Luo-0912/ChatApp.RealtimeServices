namespace ChatApp.Realtime.Abstractions.Messaging.History;

/// <summary>
/// 同步 catch-up 批量查询规格：After* 为空时按会话最新消息向前取。
/// AfterChangedAtMs 为变更水位（changed_at_ms），涵盖消息插入/编辑/撤回/Reaction。
/// </summary>
public sealed class HistoryCatchUpQuery
{
    public required string ConversationId { get; init; }
    public long? AfterChangedAtMs { get; init; }
    public string? AfterMessageId { get; init; }
    public int Take { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class DeviceSyncCursor
{
    public required string ConversationId { get; init; }
    /// <summary>
    /// Reliability-1：变更水位（changed_at_ms），涵盖消息插入/编辑/撤回/Reaction。
    /// 不再使用 ReceivedAt 命名，避免与 received_at_ms（仅插入时间）混淆。
    /// </summary>
    public long AfterChangedAtMs { get; init; }
    public required string AfterMessageId { get; init; }
}

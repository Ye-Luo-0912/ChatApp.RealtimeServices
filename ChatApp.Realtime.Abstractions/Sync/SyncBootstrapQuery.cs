namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class SyncBootstrapQuery
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }

    /// <summary>
    /// 设备哈希：用于加载/持久化设备级同步游标；与用户级已读无关。
    /// </summary>
    public ulong? DeviceIdHash { get; init; }

    public int ListLimit { get; init; } = 50;
    public int HistoryLimitPerConversation { get; init; } = 20;
    public int MaxConversationsWithHistory { get; init; } = 10;
    public IReadOnlyList<ConversationSyncWatermark>? Watermarks { get; init; }
}

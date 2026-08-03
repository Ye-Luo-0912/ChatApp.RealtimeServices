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

    /// <summary>
    /// 关系列表增量同步水位。客户端可按 list_type 维度提供本地水位。
    /// <para>
    /// null 或空表示不请求关系同步（仅会话同步）。
    /// </para>
    /// </summary>
    public IReadOnlyList<RelationshipSyncWatermark>? RelationshipWatermarks { get; init; }

    /// <summary>关系列表分页大小。null 或 0 表示默认值 50。</summary>
    public int? RelationshipListLimit { get; init; }
}
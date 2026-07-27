namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class MessageHistoryQuery
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public long? BeforeReceivedAtMs { get; init; }
    public string? BeforeMessageId { get; init; }
    public int Limit { get; init; } = 50;

    /// <summary>
    /// 非空时按会话查询。
    /// <para>P0-3：不再支持空 ConversationId 的用户级全量历史（群消息 receiver_user_id=0
    /// 导致全局查询遗漏群消息）。所有列表查询必须带 ConversationId。</para>
    /// </summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 向前（更新）翻页起点（变更水位），与 Before* 互斥；仅会话历史支持。
    /// <para>P0-2：After 模式按 changed_at_ms 过滤和排序，因此游标携带的是变更水位而非接收时间。</para>
    /// </summary>
    public long? AfterChangedAtMs { get; init; }

    public string? AfterMessageId { get; init; }

    /// <summary>非空时按消息 Id 精确查询（用于审核证据等）；需 UserId 为发送方或接收方。</summary>
    public string? MessageId { get; init; }
}

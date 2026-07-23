namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class MessageHistoryQuery
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public long? BeforeReceivedAtMs { get; init; }
    public string? BeforeMessageId { get; init; }
    public int Limit { get; init; } = 50;

    /// <summary>
    /// 非空时按会话查询；空则保持用户级全量历史（兼容旧客户端）。
    /// </summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 向前（更新）翻页起点，与 Before* 互斥；仅会话历史支持。
    /// </summary>
    public long? AfterReceivedAtMs { get; init; }

    public string? AfterMessageId { get; init; }

    /// <summary>非空时按消息 Id 精确查询（用于审核证据等）；需 UserId 为发送方或接收方。</summary>
    public string? MessageId { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class MessageHistoryQuery
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public long? BeforeReceivedAtMs { get; init; }
    public string? BeforeMessageId { get; init; }
    public int Limit { get; init; } = 50;

    /// <summary>非空时按消息 Id 精确查询（用于审核证据等）；需 UserId 为发送方或接收方。</summary>
    public string? MessageId { get; init; }
}

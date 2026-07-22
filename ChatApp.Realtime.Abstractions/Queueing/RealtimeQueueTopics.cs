namespace ChatApp.Realtime.Abstractions.Queueing;

/// <summary>
/// 实时服务内部使用的消息主题名称。
/// 当前 NATS 实现会把这些名称作为 subject 使用。
/// </summary>
public sealed class RealtimeQueueTopics
{
    public required string IncomingMessages { get; init; }
    public required string MessageReceipts { get; init; }
    public required string RealtimeEvents { get; init; }
    /// <summary>账号删除清理专用 subject，避免清理消费者 ACK 无关网关事件。</summary>
    public string AccountCleanup { get; init; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueries { get; init; } = "chat.message-history.query";
    public string? MessagePersistence { get; init; }
    public string DeadLetters { get; init; } = "chat.dead-letters";
}

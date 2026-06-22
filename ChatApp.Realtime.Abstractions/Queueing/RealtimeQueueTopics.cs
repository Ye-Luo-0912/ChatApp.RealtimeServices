namespace ChatApp.Realtime.Abstractions.Queueing;

/// <summary>
/// 实时服务内部使用的消息主题名称。
/// 当前 NATS 实现会把这些名称作为 subject 使用。
/// </summary>
public sealed class RealtimeQueueTopics
{
    public required string IncomingMessages { get; init; }
    public required string RealtimeEvents { get; init; }
    public string? MessagePersistence { get; init; }
}

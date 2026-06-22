namespace ChatApp.Realtime.Abstractions.Queueing;

/// <summary>
/// 实时队列运行时配置。
/// Provider 决定基础设施层注册哪一种队列实现；Endpoint 是该实现的连接地址。
/// </summary>
public sealed class RealtimeQueueOptions
{
    public required string Provider { get; init; }
    public string? Endpoint { get; init; }
    public required string ConsumerGroup { get; init; }
    public required RealtimeQueueTopics Topics { get; init; }
}

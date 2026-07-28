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

    /// <summary>
    /// Realtime Event 分片 subject 模板（如 <c>chat.realtime-events.{0}</c>）。
    /// <para>
    /// 配置后，Server 端事件发布器将按目标用户的在线 Gateway 集合定向投递，
    /// 而非全量广播。留空则保持广播模式（默认）。
    /// </para>
    /// </summary>
    public string? RealtimeEventsShardSubjectPattern { get; set; }

    /// <summary>
    /// 分片发布有限并行度：向多个 Gateway shard 定向投递时的最大并发发布数。
    /// <para>
    /// 默认 4，避免无界 <c>Task.WhenAll</c> 在大扇出场景下打爆 NATS 连接。
    /// 单 shard 路径保持顺序 await（无并发开销）。小于 1 时按 1 处理。
    /// </para>
    /// </summary>
    public int ShardPublishParallelism { get; set; } = 4;
}

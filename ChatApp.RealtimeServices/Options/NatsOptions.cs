namespace ChatApp.RealtimeServices.Options;

/// <summary>
/// NATS 队列配置。
/// P1 阶段先使用 Core NATS 的发布订阅和 queue group，后续需要消息确认、重放和死信时再切到 JetStream。
/// </summary>
public sealed class NatsOptions
{
    public string? Url { get; init; }
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
}

/// <summary>
/// NATS subject 配置。
/// subject 是 NATS 的路由地址，这里保持和原实时消息边界一一对应。
/// </summary>
public sealed class NatsSubjectOptions
{
    public required string IncomingMessages { get; init; }
    public required string RealtimeEvents { get; init; }
    public string? MessagePersistence { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 代表实时事件的类，用于在应用程序中传递即时发生的事件信息。
/// 该类包含了事件的基本属性，如事件ID、类型、目标用户ID等，并支持可选的执行者用户ID、会话ID以及负载JSON字符串。
/// </summary>
/// <remarks>
/// 此类是密封的（sealed），意味着它不能被继承。主要用于通过<see cref="IRealtimeEventPublisher"/>接口发布或由<see cref="IRealtimeEventConsumer"/>接口消费的场景。
/// 事件的时间戳默认设置为创建实例时的UTC时间毫秒数。
/// </remarks>
public sealed class RealtimeEvent
{
    public required string EventId { get; init; }
    public required RealtimeEventType Type { get; init; }

    public required long TargetUserId { get; init; }
    public long? ActorUserId { get; init; }

    public string? SessionId { get; init; }
    public string? PayloadJson { get; init; }

    public long OccurredAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

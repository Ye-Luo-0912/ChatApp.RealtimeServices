using System.Text.Json.Serialization;

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

    public string? MessageId { get; init; }

    public string? SessionId { get; init; }
    public string? PayloadJson { get; init; }

    // W3C trace context is persisted inside the existing Outbox JSON payload.
    // Both fields are optional to remain compatible with older events.
    public string? TraceParent { get; init; }
    public string? TraceState { get; init; }

    public long OccurredAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>
    /// 多目标投递列表（群聊聚合事件使用）。非空时按此列表遍历本机会话投递；
    /// 为空时回退到 <see cref="TargetUserId"/> 单目标路径。
    /// </summary>
    public long[]? TargetUserIds { get; init; }

    /// <summary>
    /// P1-4：应用层（如 <c>DefaultIncomingMessageProcessor</c>）可将已构造的 payload 对象通过此属性
    /// 直接传给 <c>IRealtimeMessageStore</c>，避免在 Processor 中先序列化、Store 又反序列化回对象
    /// 才能绑定附件。Store 在最终写入 Outbox 前会调用 <c>EnrichChatMessagePayload</c> 物化一次得到
    /// <see cref="PayloadJson"/>，物化后应清空此引用。
    /// <para>
    /// 该字段不参与 JSON 序列化：Outbox 表中的 <c>payload_json</c> 仅保存物化后的 <see cref="PayloadJson"/>。
    /// </para>
    /// </summary>
    [JsonIgnore]
    public object? Payload { get; init; }
}

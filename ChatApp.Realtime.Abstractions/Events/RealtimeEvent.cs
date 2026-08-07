using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Routing;

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
    /// 多目标投递列表（群聊聚合或单聊接收方 + 发送方多设备回声使用）。非空时按此列表遍历本机会话投递；
    /// 为空时回退到 <see cref="TargetUserId"/> 单目标路径。
    /// </summary>
    public long[]? TargetUserIds { get; init; }

    /// <summary>
    /// Perf-2：事件受众类型，决定 Publisher 的路由策略。
    /// <para>
    /// <see cref="AudienceKind.Conversation"/> 时使用 <see cref="ConversationId"/>
    /// 通过 <c>IConversationGatewayDirectory</c> 一次查询会话在线 Gateway 实例集合，
    /// 替代逐用户查询 <see cref="TargetUserIds"/>。
    /// </para>
    /// <para>
    /// 默认 null 等同 <see cref="AudienceKind.User"/>，兼容历史路径。
    /// </para>
    /// </summary>
    public AudienceKind? AudienceKind { get; init; }

    /// <summary>
    /// Perf-2：会话级路由使用的会话编号。
    /// 仅当 <see cref="AudienceKind"/> = <see cref="Routing.AudienceKind.Conversation"/> 时使用。
    /// </summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 极限-3：会话级广播时排除的用户编号。
    /// <para>
    /// 仅当 <see cref="AudienceKind"/> = <see cref="Routing.AudienceKind.Conversation"/> 时有效。
    /// 典型场景：群 MarkRead 广播——读者本人不需要再收到自己的已读水位通知，
    /// 通过本字段让 Gateway 在投递时跳过该用户的所有会话，无需物化 N-1 个 TargetUserIds。
    /// </para>
    /// <para>
    /// Gateway 收到事件后检查本地会话所属用户是否等于 <see cref="ExcludeUserId"/>，
    /// 若是则跳过该会话投递。其余成员正常投递。
    /// </para>
    /// </summary>
    public long? ExcludeUserId { get; init; }

    /// <summary>
    /// 四-2：线协议版本号。
    /// <para>
    /// null 表示 v1（历史事件），新事件默认为 <see cref="RealtimeProtocolVersions.Current"/>。
    /// Gateway 据此判断客户端是否支持该事件的字段，实现滚动兼容（四-3）。
    /// </para>
    /// </summary>
    public int? ProtocolVersion { get; init; }

    /// <summary>
    /// 四-1：会话受众版本号。
    /// <para>
    /// 仅当 <see cref="AudienceKind"/> = <see cref="Routing.AudienceKind.Conversation"/> 时有效。
    /// 每次群成员变更（加人/踢人/离群/解散）时递增，Gateway 据此判断本地 audience 缓存是否过期。
    /// 版本号不匹配时 Gateway 重新拉取 audience 列表。
    /// </para>
    /// </summary>
    public long? AudienceVersion { get; init; }

    /// <summary>
    /// 四-3：该事件所需的最小协议版本。
    /// <para>
    /// null 表示 v1（所有客户端均可处理）。用于标记引入新字段或新语义的事件类型，
    /// Gateway 在投递时检查客户端协议版本，低于此版本时优雅跳过（不投递）。
    /// </para>
    /// </summary>
    public int? MinProtocolVersion { get; init; }

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

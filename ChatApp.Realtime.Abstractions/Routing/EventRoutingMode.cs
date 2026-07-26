namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// Realtime Event 投递路由模式。
/// <para>
/// 第三阶段引入分片路由，逐步从全量广播迁移到按 Gateway 实例定向投递。
/// </para>
/// </summary>
public enum EventRoutingMode
{
    /// <summary>
    /// 广播模式（默认，向后兼容）：每个 Gateway 实例订阅全量 subject，
    /// 收到所有事件后在本地按 <c>TargetUserId</c> 过滤。
    /// <para>
    /// 成本 = 全局事件量 × Gateway 实例数。少量实例时简单可靠。
    /// </para>
    /// </summary>
    Broadcast = 0,

    /// <summary>
    /// 分片模式：每个 Gateway 实例订阅自己的分片 subject
    /// （如 <c>chat.realtime-events.{instanceId}</c>），
    /// 发布方通过 <c>IGatewayDirectory</c> 查询目标用户的在线 Gateway 集合后定向投递。
    /// <para>
    /// 成本 = 实际投递事件量（仅目标用户在线的 Gateway 收到）。
    /// Gateway 实例数增加后成本不再线性增长。
    /// </para>
    /// <para>
    /// 发布方查询失败或用户离线时回退到广播 subject，保证不丢事件。
    /// </para>
    /// </summary>
    Sharded = 1
}

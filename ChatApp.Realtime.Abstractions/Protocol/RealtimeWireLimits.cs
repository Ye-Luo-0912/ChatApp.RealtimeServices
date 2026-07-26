namespace ChatApp.Realtime.Abstractions.Protocol;

/// <summary>
/// 与 TcpGateway <c>PacketProtocol.MaxPayloadSize</c>（80 KiB）对齐的统一响应字节预算。
/// 留出帧头与 JSON 包装余量，避免单帧无法发出。
/// </summary>
public static class RealtimeWireLimits
{
    /// <summary>Gateway 单帧载荷上限（字节）。</summary>
    public const int GatewayMaxPayloadBytes = 80 * 1024;

    /// <summary>业务查询响应统一预算，必须严格小于 <see cref="GatewayMaxPayloadBytes"/>。</summary>
    public const int MaximumResponseBytes = 64 * 1024;

    /// <summary>
    /// 打包时相对 <see cref="MaximumResponseBytes"/> 预留的余量，覆盖 JSON 键名/标点与估算误差。
    /// </summary>
    public const int ResponsePackingSafetyMarginBytes = 2 * 1024;

    /// <summary>实际打包循环使用的预算上限。</summary>
    public const int PackingBudgetBytes = MaximumResponseBytes - ResponsePackingSafetyMarginBytes;

    // ---- 群成员事件聚合硬限制 ----

    /// <summary>
    /// 单个聚合事件 <see cref="Events.RealtimeEvent.TargetUserIds"/> 的最大长度。
    /// 与 <c>NpgsqlRealtimeGroupStore.MaxMembersPerGroup</c> 对齐，覆盖满员群全员投递。
    /// </summary>
    public const int MaxTargetUserIdsPerEvent = 200;

    /// <summary>
    /// 单次群成员变更（建群 / 加人）的最大人数。
    /// 超过该值的请求必须在应用层分批，避免单事务事件过大。
    /// </summary>
    public const int MaxMembersPerGroupChange = 50;

    /// <summary>
    /// 单个 Outbox payload（<c>payload_json</c>）的最大字节数。
    /// 聚合事件携带 TargetUserIds 与 Members 列表，需防止异常大 payload 撑爆 JetStream 消息上限。
    /// </summary>
    public const int MaxOutboxPayloadBytes = 256 * 1024;

    /// <summary>
    /// 单事务内写入 Outbox 事件的最大条数。
    /// 聚合后建群事件数应远低于该值；超过则视为异常并拒绝事务。
    /// </summary>
    public const int MaxEventsPerTransaction = 1_000;
}

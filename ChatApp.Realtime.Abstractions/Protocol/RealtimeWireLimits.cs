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
}

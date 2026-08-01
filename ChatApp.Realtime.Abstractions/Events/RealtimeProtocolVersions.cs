namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 四-2/四-3：实时事件线协议版本常量。
/// <para>
/// 版本演进策略：
/// <list type="bullet">
/// <item>v1：初始版本，所有历史事件均为 v1（ProtocolVersion 字段为 null 时视为 v1）。</item>
/// <item>v2：引入 audience_version 和 MinProtocolVersion 字段后的版本。</item>
/// </list>
/// </para>
/// <para>
/// 滚动兼容策略（四-3）：
/// <list type="bullet">
/// <item>服务端始终接受 v1 事件（向前兼容）。</item>
/// <item>新事件类型可标记 MinProtocolVersion，Gateway 据此对旧版本客户端跳过投递。</item>
/// <item>协议版本协商在 Gateway 连接握手时完成（跨仓库），本仓库仅在事件中携带版本信息。</item>
/// </list>
/// </para>
/// </summary>
public static class RealtimeProtocolVersions
{
    /// <summary>
    /// 初始协议版本。所有历史事件（ProtocolVersion 为 null）均视为 v1。
    /// </summary>
    public const int V1 = 1;

    /// <summary>
    /// 四-2：当前协议版本。引入 audience_version 和 MinProtocolVersion 字段。
    /// </summary>
    public const int V2 = 2;

    /// <summary>
    /// 四-2：服务端默认使用的协议版本（新事件默认标记为此版本）。
    /// </summary>
    public const int Current = V2;

    /// <summary>
    /// 四-3：服务端支持的最小协议版本（用于校验入站事件的兼容性）。
    /// </summary>
    public const int MinSupported = V1;

    /// <summary>
    /// 四-3：判断指定协议版本是否被服务端支持。
    /// </summary>
    public static bool IsSupported(int? version) =>
        version is null || version >= MinSupported;
}

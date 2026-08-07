namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// P1-1：契约包序列化元数据。用于跨仓库（TcpGateway ↔ RealtimeServices）版本化契约的
/// 一致性校验与 SemVer 兼容性声明。任何破坏性契约变更必须同步递增版本并遵循 SemVer。
/// </summary>
public static class RealtimeContractMetadata
{
    /// <summary>契约包 SemVer 版本（与 ChatApp.Realtime.Contracts 包版本保持一致）。</summary>
    public const string PackageVersion = "2.3.0";

    /// <summary>当前线协议版本（与 <see cref="RealtimeProtocolVersions.Current"/> 对齐）。</summary>
    public const int ProtocolVersion = RealtimeProtocolVersions.V2;

    /// <summary>最小支持的线协议版本。</summary>
    public const int MinSupportedProtocolVersion = RealtimeProtocolVersions.MinSupported;

    /// <summary>序列化格式标识（camelCase JSON）。</summary>
    public const string SerializerFormat = "chatapp.realtime.json.camelCase";

    /// <summary>
    /// 序列化格式版本。major 变更（不兼容）→ 契约包 major 递增；minor 变更（兼容新增字段）→ minor 递增。
    /// </summary>
    public const int SerializerFormatVersion = 1;
}

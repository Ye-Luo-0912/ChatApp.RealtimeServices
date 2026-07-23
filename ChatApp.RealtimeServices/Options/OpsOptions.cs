namespace ChatApp.RealtimeServices.Options;

/// <summary>
/// Realtime 运维 HTTP（/ops/*）访问控制。
/// Production 建议配置 ApiKey；未配置时仅允许非 Production（开发/测试）。
/// </summary>
public sealed class OpsOptions
{
    public const string SectionName = "Ops";

    /// <summary>请求头 <c>X-Ops-Api-Key</c> 须与此值匹配；为空则依赖环境门禁。</summary>
    public string? ApiKey { get; set; }
}

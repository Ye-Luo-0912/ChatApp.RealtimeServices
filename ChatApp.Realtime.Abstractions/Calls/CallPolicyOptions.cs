namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话控制策略：有界 TTL、信令预算与载荷上限。所有值必须有界，防止控制面状态泄漏。
/// </summary>
public sealed class CallPolicyOptions
{
    public const string SectionName = "CallControl";

    /// <summary>振铃超时（ms）。到期后由 Server 驱动 Timeout → Ended(Missed)。</summary>
    public long RingingTimeoutMs { get; init; } = 30_000;

    /// <summary>通话最大时长（ms）。到期后 Timeout → Ended(TimedOut)。</summary>
    public long ActiveMaxDurationMs { get; init; } = 2 * 60 * 60 * 1000;

    /// <summary>终态幂等墓碑保留时长（ms）。结束后仅短暂保留以吸收重放，然后清理。</summary>
    public long EndedRetentionMs { get; init; } = 30_000;

    /// <summary>单条 SDP 载荷最大字节数（预算）。</summary>
    public int MaxSdpBytes { get; init; } = 64 * 1024;

    /// <summary>单次通话最大信令条数（预算）。</summary>
    public int MaxSignalsPerCall { get; init; } = 256;

    /// <summary>grant 预留宽限（ms），用于过期判定缓冲。</summary>
    public long GrantExpiryGraceMs { get; init; } = 500;

    /// <summary>
    /// 状态清理宽限（ms）。存储 TTL 比状态逻辑超时多保留该时长，
    /// 为处理器驱动显式超时终态（Ringing→Missed / Active→TimedOut）留出窗口。
    /// </summary>
    public long CleanupGraceMs { get; init; } = 1000;
}
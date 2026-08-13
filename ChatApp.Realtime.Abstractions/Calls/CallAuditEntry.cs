namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话审计记录。只保存参与者、状态、时间、失败分类与必要 QoE 汇总——
/// 绝不包含 SDP/ICE 载荷、音频数据或内部路由细节。
/// </summary>
public sealed record CallAuditEntry
{
    public required string CallId { get; init; }

    public required long CallerUserId { get; init; }

    public required long CalleeUserId { get; init; }

    public required CallState State { get; init; }

    public required CallEndReason EndReason { get; init; }

    public required long Revision { get; init; }

    /// <summary>该审计对应的事件类型（迁移/失败）。</summary>
    public required CallAuditEventKind EventKind { get; init; }

    /// <summary>失败分类；成功时为 null。</summary>
    public CallErrorCode? FailureCode { get; init; }

    /// <summary>QoE 汇总：信令条数（SDP 转发次数）。必要的最小集合。</summary>
    public int SignalCount { get; init; }

    /// <summary>发生时间（Unix 毫秒）。</summary>
    public required long OccurredAtMs { get; init; }
}

/// <summary>
/// 审计事件分类。
/// </summary>
public enum CallAuditEventKind : byte
{
    Transition = 1,
    Terminal = 2,
    Failure = 3,
    Timeout = 4,
}
namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话控制错误码。语义固定，客户端据此处理重复、乱序、过期、授权和竞态。
/// </summary>
public enum CallErrorCode : byte
{
    None = 0,

    /// <summary>命令编号缺失或超长。</summary>
    InvalidCommandId = 1,

    /// <summary>call id 缺失或超长。</summary>
    InvalidCallId = 2,

    /// <summary>通话参与者（主叫/被叫）缺失或无效。</summary>
    InvalidParticipant = 3,

    /// <summary>call grant 缺失、过期或签名/参与者校验失败（fail-closed）。</summary>
    GrantInvalid = 4,

    /// <summary>call grant 已过期。</summary>
    GrantExpired = 5,

    /// <summary>SDP 载荷缺失或超过预算上限。</summary>
    SdpInvalid = 6,

    /// <summary>该命令在当前状态下不允许（迁移表拒绝）。</summary>
    InvalidTransition = 7,

    /// <summary>命令 revision 非单调（乱序 / 过期），已拒绝。</summary>
    RevisionStale = 8,

    /// <summary>通话不存在或已结束（终态后收到新命令）。</summary>
    CallEnded = 9,

    /// <summary>临时信令路径预算耗尽（信号条数超限）。</summary>
    SignalBudgetExceeded = 10,

    /// <summary>并发竞态导致 CAS 冲突，需重试。</summary>
    ConflictRetry = 11,

    /// <summary>临时状态仓储不可用（fail-closed）。</summary>
    StateStoreUnavailable = 12,
}

/// <summary>
/// <see cref="CallErrorCode"/> 的稳定线协议字符串映射。
/// </summary>
public static class CallErrorCodeExtensions
{
    public static string ToStableCode(this CallErrorCode code) => code switch
    {
        CallErrorCode.None => "none",
        CallErrorCode.InvalidCommandId => "call_invalid_command_id",
        CallErrorCode.InvalidCallId => "call_invalid_call_id",
        CallErrorCode.InvalidParticipant => "call_invalid_participant",
        CallErrorCode.GrantInvalid => "call_grant_invalid",
        CallErrorCode.GrantExpired => "call_grant_expired",
        CallErrorCode.SdpInvalid => "call_sdp_invalid",
        CallErrorCode.InvalidTransition => "call_invalid_transition",
        CallErrorCode.RevisionStale => "call_revision_stale",
        CallErrorCode.CallEnded => "call_ended",
        CallErrorCode.SignalBudgetExceeded => "call_signal_budget_exceeded",
        CallErrorCode.ConflictRetry => "call_conflict_retry",
        CallErrorCode.StateStoreUnavailable => "call_state_store_unavailable",
        _ => "call_unknown"
    };
}
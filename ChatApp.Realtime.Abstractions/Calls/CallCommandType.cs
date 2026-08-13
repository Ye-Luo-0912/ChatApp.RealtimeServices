namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话信令命令类型。每个命令以 call id + command id 幂等、单调 revision 排序，
/// 由 <see cref="CallStateMachine"/> 校验状态迁移合法性。
/// <para>
/// SDP/ICE 只随 SignalCarrying 命令转发，且只允许走受预算限制的临时信令路径，
/// 不进入 PostgreSQL、持久化 Outbox 或 JetStream 历史。
/// </para>
/// </summary>
public enum CallCommandType : byte
{
    /// <summary>主叫发起呼叫（携带 SDP offer）。<see cref="CallState.Idle"/> → <see cref="CallState.Ringing"/>。</summary>
    Invite = 1,

    /// <summary>被叫设备上报振铃（可选 ack，不携带 SDP）。状态不变。</summary>
    Ringing = 2,

    /// <summary>被叫接受呼叫（携带 SDP answer）。<see cref="CallState.Ringing"/> → <see cref="CallState.Active"/>。</summary>
    Accept = 3,

    /// <summary>被叫拒绝呼叫。<see cref="CallState.Ringing"/> → <see cref="CallState.Ended"/>（Rejected）。</summary>
    Reject = 4,

    /// <summary>主叫在接通前取消。<see cref="CallState.Ringing"/> → <see cref="CallState.Ended"/>（Cancelled）。</summary>
    Cancel = 5,

    /// <summary>任一方挂断。<see cref="CallState.Ringing"/> 或 <see cref="CallState.Active"/> → <see cref="CallState.Ended"/>（HungUp）。</summary>
    End = 6,

    /// <summary>
    /// 断线后重连，重新建立临时 SDP/ICE 路由路径。要求 grant 仍有效、revision 单调。
    /// 状态不变（Ringing/Active），仅刷新路由与 TTL。
    /// </summary>
    Reconnect = 7,

    /// <summary>服务端驱动：TTL 到期（振铃超时 → Missed；通话上限 → TimedOut）。</summary>
    Timeout = 8,
}
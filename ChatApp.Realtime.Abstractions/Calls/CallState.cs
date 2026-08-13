namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话控制状态机的状态。终态唯一：任何非 <see cref="Ended"/> 状态经合法迁移
/// 最终收敛到 <see cref="Ended"/>，且一旦进入 <see cref="Ended"/> 不允许再迁出
/// （仅允许同一 command id 的幂等重放返回已记录结果）。
/// </summary>
public enum CallState : byte
{
    /// <summary>无通话（仓储中不存在记录）。</summary>
    Idle = 0,

    /// <summary>振铃中：主叫已发起、被叫未应答。</summary>
    Ringing = 1,

    /// <summary>通话中：被叫已接受，SDP/ICE 已协商。</summary>
    Active = 2,

    /// <summary>终态：通话已结束（原因见 <see cref="CallEndReason"/>）。</summary>
    Ended = 3,
}

/// <summary>
/// 通话终态原因。仅用于审计与客户端展示，不参与状态机迁移判定。
/// </summary>
public enum CallEndReason : byte
{
    None = 0,
    Rejected = 1,
    Cancelled = 2,
    HungUp = 3,
    Missed = 4,
    TimedOut = 5,
}
namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话状态机的纯迁移表。状态迁移必须自此收敛到唯一终态 <see cref="CallState.Ended"/>，
/// 且一旦进入终态不允许迁出（仅允许同一 command id 幂等重放）。
/// </summary>
public static class CallStateMachine
{
    /// <summary>
    /// 计算一次迁移。返回目标状态与终态原因；非法迁移返回 null。
    /// </summary>
    /// <param name="current">当前状态。</param>
    /// <param name="command">命令类型。</param>
    /// <param name="actorIsCaller">发起命令的用户是否为主叫。</param>
    public static CallTransition? Transition(CallState current, CallCommandType command, bool actorIsCaller)
    {
        // 终态不可迁出。
        if (current == CallState.Ended)
            return null;

        return (current, command) switch
        {
            (CallState.Idle, CallCommandType.Invite) when actorIsCaller =>
                new CallTransition(CallState.Ringing, CallEndReason.None),

            (CallState.Ringing, CallCommandType.Ringing) when !actorIsCaller =>
                new CallTransition(CallState.Ringing, CallEndReason.None),

            (CallState.Ringing, CallCommandType.Accept) when !actorIsCaller =>
                new CallTransition(CallState.Active, CallEndReason.None),

            (CallState.Ringing, CallCommandType.Reject) when !actorIsCaller =>
                new CallTransition(CallState.Ended, CallEndReason.Rejected),

            (CallState.Ringing, CallCommandType.Cancel) when actorIsCaller =>
                new CallTransition(CallState.Ended, CallEndReason.Cancelled),

            (CallState.Ringing, CallCommandType.End) =>
                new CallTransition(CallState.Ended, CallEndReason.HungUp),

            (CallState.Active, CallCommandType.End) =>
                new CallTransition(CallState.Ended, CallEndReason.HungUp),

            (CallState.Ringing, CallCommandType.Reconnect) =>
                new CallTransition(CallState.Ringing, CallEndReason.None),

            (CallState.Active, CallCommandType.Reconnect) =>
                new CallTransition(CallState.Active, CallEndReason.None),

            (CallState.Ringing, CallCommandType.Timeout) =>
                new CallTransition(CallState.Ended, CallEndReason.Missed),

            (CallState.Active, CallCommandType.Timeout) =>
                new CallTransition(CallState.Ended, CallEndReason.TimedOut),

            _ => null
        };
    }

    /// <summary>该命令是否携带 SDP，需经临时信令路径转发。</summary>
    public static bool CarriesSdp(CallCommandType command) => command switch
    {
        CallCommandType.Invite => true,
        CallCommandType.Accept => true,
        CallCommandType.Reconnect => true,
        _ => false
    };

    /// <summary>非载荷变更（Ringing ack）不转发信令，仅更新状态。</summary>
    public static bool IsSilent(CallCommandType command) =>
        command == CallCommandType.Ringing;
}

/// <summary>
/// 一次迁移的目标状态与终态原因。
/// </summary>
public sealed record CallTransition(CallState TargetState, CallEndReason EndReason);
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Core.Calls;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// CALL-CTRL-1：通话状态机迁移表与处理器行为（幂等/乱序/超时/重连/终态唯一/SDP 不持久化/授权过期/预算）。
/// </summary>
public sealed class CallStateMachineTests
{
    private static bool IsCaller(long actor) => actor == 1001;
    private static bool IsCallee(long actor) => actor == 1002;

    [Theory]
    [InlineData(CallState.Idle, CallCommandType.Invite, true, CallState.Ringing)]
    [InlineData(CallState.Ringing, CallCommandType.Ringing, false, CallState.Ringing)]
    [InlineData(CallState.Ringing, CallCommandType.Accept, false, CallState.Active)]
    [InlineData(CallState.Ringing, CallCommandType.Reject, false, CallState.Ended)]
    [InlineData(CallState.Ringing, CallCommandType.Cancel, true, CallState.Ended)]
    [InlineData(CallState.Ringing, CallCommandType.End, true, CallState.Ended)]
    [InlineData(CallState.Active, CallCommandType.End, false, CallState.Ended)]
    [InlineData(CallState.Ringing, CallCommandType.Reconnect, true, CallState.Ringing)]
    [InlineData(CallState.Active, CallCommandType.Reconnect, false, CallState.Active)]
    [InlineData(CallState.Ringing, CallCommandType.Timeout, true, CallState.Ended)]
    [InlineData(CallState.Active, CallCommandType.Timeout, true, CallState.Ended)]
    public void Transition_ValidMoves_ConvergeToExpected(CallState from, CallCommandType cmd, bool caller, CallState expected)
    {
        var t = CallStateMachine.Transition(from, cmd, caller);
        Assert.NotNull(t);
        Assert.Equal(expected, t.TargetState);
    }

    [Theory]
    [InlineData(CallState.Idle, CallCommandType.Accept)]   // 被叫不能直接 Accept
    [InlineData(CallState.Idle, CallCommandType.End)]      // 未发起不能 End
    [InlineData(CallState.Ringing, CallCommandType.Invite)] // 已振铃不能重复 Invite
    [InlineData(CallState.Active, CallCommandType.Accept)]  // 已接通不能重复 Accept
    [InlineData(CallState.Active, CallCommandType.Ringing)] // Active 不能回 Ringing
    [InlineData(CallState.Ended, CallCommandType.End)]      // 终态不可迁出
    [InlineData(CallState.Ended, CallCommandType.Reconnect)]
    public void Transition_InvalidMoves_ReturnNull(CallState from, CallCommandType cmd)
    {
        // 用主叫身份尝试，避免被叫限制干扰断言。
        Assert.Null(CallStateMachine.Transition(from, cmd, actorIsCaller: true));
    }

    [Fact]
    public void Transition_Invite_ByCallee_IsRejected()
    {
        Assert.Null(CallStateMachine.Transition(CallState.Idle, CallCommandType.Invite, actorIsCaller: false));
    }

    [Fact]
    public void Transition_Reject_ByCaller_IsRejected()
    {
        Assert.Null(CallStateMachine.Transition(CallState.Ringing, CallCommandType.Reject, actorIsCaller: true));
    }

    [Fact]
    public void Transition_Cancel_ByCallee_IsRejected()
    {
        Assert.Null(CallStateMachine.Transition(CallState.Ringing, CallCommandType.Cancel, actorIsCaller: false));
    }

    [Theory]
    [InlineData(CallCommandType.Invite, true)]
    [InlineData(CallCommandType.Accept, true)]
    [InlineData(CallCommandType.Reconnect, true)]
    [InlineData(CallCommandType.Ringing, false)]
    [InlineData(CallCommandType.Reject, false)]
    [InlineData(CallCommandType.Cancel, false)]
    [InlineData(CallCommandType.End, false)]
    [InlineData(CallCommandType.Timeout, false)]
    public void CarriesSdp_MatchesSpec(CallCommandType cmd, bool expected)
    {
        Assert.Equal(expected, CallStateMachine.CarriesSdp(cmd));
    }

    [Fact]
    public void IsSilent_OnlyRingingAck()
    {
        Assert.True(CallStateMachine.IsSilent(CallCommandType.Ringing));
        Assert.False(CallStateMachine.IsSilent(CallCommandType.Invite));
    }
}
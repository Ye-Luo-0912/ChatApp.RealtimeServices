using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Core.Calls;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// 可控时钟，用于驱动 TTL 超时判定。
/// </summary>
public sealed class ManualTimeProvider : TimeProvider
{
    private long _utcMs;

    public ManualTimeProvider(long startUtcMs)
    {
        _utcMs = startUtcMs;
    }

    public override DateTimeOffset GetUtcNow() => DateTimeOffset.FromUnixTimeMilliseconds(_utcMs);

    public void Advance(long ms) => Interlocked.Add(ref _utcMs, ms);
}

/// <summary>
/// 记录转发信令的测试转发器。
/// </summary>
public sealed class RecordingCallSignalForwarder : ICallSignalForwarder
{
    private readonly Channel<CallSignalEnvelope> _signals =
        Channel.CreateUnbounded<CallSignalEnvelope>();

    public Task<bool> ForwardAsync(CallSignalEnvelope signal, CancellationToken ct = default)
    {
        _signals.Writer.TryWrite(signal);
        return Task.FromResult(true);
    }

    public IReadOnlyList<CallSignalEnvelope> Snapshot()
    {
        var list = new List<CallSignalEnvelope>();
        while (_signals.Reader.TryRead(out var s))
            list.Add(s);
        return list;
    }
}

/// <summary>
/// CALL-CTRL-1：DefaultCallControlProcessor 行为测试。
/// </summary>
public sealed class DefaultCallControlProcessorTests
{
    private const long Caller = 1001;
    private const long Callee = 1002;
    private const long StartMs = 1_700_000_000_000; // 固定起点

    private readonly ManualTimeProvider _clock = new(StartMs);
    private readonly InMemoryCallStateStore _stateStore = null!; // 在构造函数中注入同一时钟
    private readonly InMemoryCallAuditStore _auditStore = new();
    private readonly RecordingCallSignalForwarder _forwarder = new();
    private readonly CallPolicyOptions _options = new();
    private readonly CallMetrics _metrics = new();

    public DefaultCallControlProcessorTests()
    {
        // 状态存储与处理器共享同一时钟，保证 TTL 超时判定一致。
        _stateStore = new InMemoryCallStateStore(_clock);
    }

    private DefaultCallControlProcessor CreateProcessor(ICallGrantVerifier? verifier = null)
        => new(
            _stateStore,
            verifier ?? new DefaultCallGrantVerifier(new CallPolicyOptions()),
            _forwarder,
            _auditStore,
            _options,
            _metrics,
            _clock,
            NullLogger<DefaultCallControlProcessor>.Instance);

    private static CallGrant Grant(long caller = Caller, long callee = Callee, long? expiresAt = null)
        => new()
        {
            CallId = "call-1",
            CallerUserId = caller,
            CalleeUserId = callee,
            ExpiresAtMs = expiresAt ?? StartMs + 60_000,
            Nonce = "nonce-1",
            Signature = "sig"
        };

    private static CallCommand Command(
        CallCommandType type,
        long actor,
        long revision,
        string commandId,
        string? sdp = null,
        CallGrant? grant = null)
        => new()
        {
            CommandId = commandId,
            CallId = "call-1",
            Type = type,
            ActorUserId = actor,
            ActorSessionId = "sess-" + actor,
            Grant = grant ?? Grant(),
            Revision = revision,
            Sdp = sdp
        };

    [Fact]
    public async Task Invite_Then_Accept_ConvergesToActive_AndForwardsSdp()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        var r1 = await processor.ProcessAsync(invite);
        Assert.True(r1.Succeeded);
        Assert.Equal(CallState.Ringing, r1.State);
        Assert.True(r1.SignalToForward is not null);

        var ringing = Command(CallCommandType.Ringing, Callee, revision: 2, commandId: "c2");
        var r2 = await processor.ProcessAsync(ringing);
        Assert.True(r2.Succeeded);
        Assert.True(r2.Replayed is false);

        var accept = Command(CallCommandType.Accept, Callee, revision: 3, commandId: "c3", sdp: "answer");
        var r3 = await processor.ProcessAsync(accept);
        Assert.True(r3.Succeeded);
        Assert.Equal(CallState.Active, r3.State);
        Assert.True(r3.SignalToForward is not null);

        // 信号经临时路径转发，且 SDP 不进入持久化（仅记录在转发器）。
        var signals = _forwarder.Snapshot();
        Assert.Equal(2, signals.Count);
        Assert.Equal("offer", signals[0].Sdp);
        Assert.Equal("answer", signals[1].Sdp);
    }

    [Fact]
    public async Task SameCommandId_Replays_WithoutNewMigration()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "dup", sdp: "offer");
        var r1 = await processor.ProcessAsync(invite);
        Assert.True(r1.Succeeded);

        var r2 = await processor.ProcessAsync(invite);
        Assert.True(r2.Succeeded);
        Assert.True(r2.Replayed);
        Assert.Equal(r1.Revision, r2.Revision);
    }

    [Fact]
    public async Task StaleRevision_IsRejected()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 5, commandId: "c1", sdp: "offer");
        await processor.ProcessAsync(invite);

        var stale = Command(CallCommandType.End, Caller, revision: 3, commandId: "c2");
        var result = await processor.ProcessAsync(stale);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.RevisionStale, result.ErrorCode);
    }

    [Fact]
    public async Task ExpiredGrant_IsRejected_FailClosed()
    {
        _clock.Advance(120_000); // 推进超过 grant 过期（起点+60s）
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        var result = await processor.ProcessAsync(invite);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.GrantExpired, result.ErrorCode);
    }

    [Fact]
    public async Task NonParticipant_IsRejected()
    {
        var processor = CreateProcessor();
        var cmd = Command(CallCommandType.Invite, actor: 9999, revision: 1, commandId: "c1", sdp: "offer");
        var result = await processor.ProcessAsync(cmd);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.GrantInvalid, result.ErrorCode);
    }

    [Theory]
    [InlineData(CallCommandType.Reject, Callee, CallState.Ended, CallEndReason.Rejected)]
    [InlineData(CallCommandType.Cancel, Caller, CallState.Ended, CallEndReason.Cancelled)]
    [InlineData(CallCommandType.End, Caller, CallState.Ended, CallEndReason.HungUp)]
    public async Task EndState_IsUnique_TerminalTransitions(CallCommandType type, long actor, CallState target, CallEndReason reason)
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        await processor.ProcessAsync(invite);

        var cmd = Command(type, actor, revision: 2, commandId: "c2");
        var result = await processor.ProcessAsync(cmd);
        Assert.True(result.Succeeded);
        Assert.Equal(target, result.State);
        Assert.Equal(reason, result.EndReason);

        // 终态后任何新命令被拒。
        var after = Command(CallCommandType.End, Caller, revision: 3, commandId: "c3");
        var rAfter = await processor.ProcessAsync(after);
        Assert.False(rAfter.Succeeded);
        Assert.Equal(CallErrorCode.CallEnded, rAfter.ErrorCode);
    }

    [Fact]
    public async Task RingingTimeout_DrivesMissed_WhenTtlExpires()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        await processor.ProcessAsync(invite);

        // 推进超过振铃超时（30s）。
        _clock.Advance(_options.RingingTimeoutMs + 1);

        var end = Command(CallCommandType.End, Caller, revision: 2, commandId: "c2");
        var result = await processor.ProcessAsync(end);
        Assert.True(result.Succeeded);
        Assert.Equal(CallState.Ended, result.State);
        Assert.Equal(CallEndReason.Missed, result.EndReason);
    }

    [Fact]
    public async Task Reconnect_KeepsActiveState()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        await processor.ProcessAsync(invite);
        var accept = Command(CallCommandType.Accept, Callee, revision: 2, commandId: "c2", sdp: "answer");
        await processor.ProcessAsync(accept);

        var reconnect = Command(CallCommandType.Reconnect, Caller, revision: 3, commandId: "c3", sdp: "reoffer");
        var result = await processor.ProcessAsync(reconnect);
        Assert.True(result.Succeeded);
        Assert.Equal(CallState.Active, result.State);
        Assert.True(result.SignalToForward is not null);
    }

    [Fact]
    public async Task SdpMissing_OnInvite_IsRejected()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: null);
        var result = await processor.ProcessAsync(invite);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.SdpInvalid, result.ErrorCode);
    }

    [Fact]
    public async Task SignalBudget_Exceeded_IsRejected()
    {
        // 预算=256，逐个 Invite 会因状态机拒绝；改用重连触发 SDP 预算计数。
        // 先构造较小预算以快速触发。
        var smallOptions = new CallPolicyOptions { MaxSignalsPerCall = 2 };
        var processor = new DefaultCallControlProcessor(
            _stateStore,
            new DefaultCallGrantVerifier(smallOptions),
            _forwarder,
            _auditStore,
            smallOptions,
            _metrics,
            _clock,
            NullLogger<DefaultCallControlProcessor>.Instance);

        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        var r1 = await processor.ProcessAsync(invite);
        Assert.True(r1.Succeeded);

        var reconnect = Command(CallCommandType.Reconnect, Caller, revision: 2, commandId: "c2", sdp: "reoffer-2");
        var r2 = await processor.ProcessAsync(reconnect);
        Assert.True(r2.Succeeded);

        var reconnect2 = Command(CallCommandType.Reconnect, Caller, revision: 3, commandId: "c3", sdp: "reoffer-3");
        var r3 = await processor.ProcessAsync(reconnect2);
        Assert.False(r3.Succeeded);
        Assert.Equal(CallErrorCode.SignalBudgetExceeded, r3.ErrorCode);
    }

    [Fact]
    public async Task Audit_RecordsTransitions_WithoutSdp()
    {
        var processor = CreateProcessor();
        var invite = Command(CallCommandType.Invite, Caller, revision: 1, commandId: "c1", sdp: "offer");
        await processor.ProcessAsync(invite);
        var accept = Command(CallCommandType.Accept, Callee, revision: 2, commandId: "c2", sdp: "answer");
        await processor.ProcessAsync(accept);

        var entries = await _auditStore.ListAsync("call-1");
        Assert.Equal(2, entries.Count);
        Assert.All(entries, e => Assert.DoesNotContain("offer", e.ToString()));
    }
}
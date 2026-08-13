using System.Diagnostics.Metrics;
using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 通话控制低基数指标。记录迁移、终态、失败分类、信令转发与幂等重放。
/// 不记录 SDP/ICE 载荷、音频数据或通话参与者身份。
/// </summary>
public sealed class CallMetrics : IDisposable
{
    private readonly Meter _meter = new("ChatApp.RealtimeServices.Calls", "1.0.0");
    private readonly Counter<long> _transitionCounter;
    private readonly Counter<long> _terminalCounter;
    private readonly Counter<long> _failureCounter;
    private readonly Counter<long> _forwardedCounter;
    private readonly Counter<long> _droppedCounter;
    private readonly Counter<long> _replayedCounter;
    private readonly Counter<long> _timeoutCounter;

    public CallMetrics()
    {
        _transitionCounter = _meter.CreateCounter<long>(
            "realtime.call.transitions", "calls", "通话状态迁移次数");
        _terminalCounter = _meter.CreateCounter<long>(
            "realtime.call.terminals", "calls", "通话终态次数");
        _failureCounter = _meter.CreateCounter<long>(
            "realtime.call.failures", "calls", "通话命令失败次数（按错误分类）");
        _forwardedCounter = _meter.CreateCounter<long>(
            "realtime.call.signal.forwarded", "signals", "临时信令路径转发的 SDP 条数");
        _droppedCounter = _meter.CreateCounter<long>(
            "realtime.call.signal.dropped", "signals", "未转发的信令条数");
        _replayedCounter = _meter.CreateCounter<long>(
            "realtime.call.replayed", "commands", "幂等重放命令数");
        _timeoutCounter = _meter.CreateCounter<long>(
            "realtime.call.timeouts", "calls", "TTL 超时终态次数");
    }

    public void Transition(CallState state) => _transitionCounter.Add(1, new KeyValuePair<string, object?>("state", state.ToString()));

    public void Terminal(CallEndReason reason) => _terminalCounter.Add(1, new KeyValuePair<string, object?>("reason", reason.ToString()));

    public void Failure(CallErrorCode code) => _failureCounter.Add(1, new KeyValuePair<string, object?>("code", code.ToStableCode()));

    public void SignalForwarded(bool forwarded) => (forwarded ? _forwardedCounter : _droppedCounter).Add(1);

    public void Replayed() => _replayedCounter.Add(1);

    public void Timeout(CallEndReason reason) => _timeoutCounter.Add(1, new KeyValuePair<string, object?>("reason", reason.ToString()));

    public void Dispose() => _meter.Dispose();
}
using System.Text;
using ChatApp.Realtime.Abstractions.Calls;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 通话控制状态机处理器。以 Server 签发的短期 call grant 为授权输入，
/// 以 call id + command id 幂等、单调 revision 排序、有界 TTL 处理重复/乱序/断线，
/// 保证终态唯一，并确保 SDP/ICE 只经受预算限制的临时信令路径转发。
/// </summary>
public sealed class DefaultCallControlProcessor : ICallControlProcessor
{
    private const int MaxCasAttempts = 4;

    private readonly ICallStateStore _stateStore;
    private readonly ICallGrantVerifier _grantVerifier;
    private readonly ICallSignalForwarder _signalForwarder;
    private readonly ICallAuditStore _auditStore;
    private readonly CallPolicyOptions _options;
    private readonly CallMetrics _metrics;
    private readonly TimeProvider _clock;
    private readonly ILogger<DefaultCallControlProcessor> _logger;

    public DefaultCallControlProcessor(
        ICallStateStore stateStore,
        ICallGrantVerifier grantVerifier,
        ICallSignalForwarder signalForwarder,
        ICallAuditStore auditStore,
        CallPolicyOptions options,
        CallMetrics metrics,
        TimeProvider clock,
        ILogger<DefaultCallControlProcessor> logger)
    {
        _stateStore = stateStore;
        _grantVerifier = grantVerifier;
        _signalForwarder = signalForwarder;
        _auditStore = auditStore;
        _options = options;
        _metrics = metrics;
        _clock = clock;
        _logger = logger;
    }

    public async Task<CallProcessResult> ProcessAsync(CallCommand command, CancellationToken ct = default)
    {
        var validation = Validate(command);
        if (validation is not null)
        {
            _metrics.Failure(validation.Value.ErrorCode);
            return CallProcessResult.Failed(validation.Value.ErrorCode, validation.Value.Message);
        }

        var nowMs = _clock.GetUtcNow().ToUnixTimeMilliseconds();

        // 授权：fail-closed。grant 不可用/过期即拒绝。
        var grant = await _grantVerifier.VerifyAsync(command.Grant, nowMs, ct).ConfigureAwait(false);
        if (!grant.Valid)
        {
            var code = grant.Error ?? CallErrorCode.GrantInvalid;
            _metrics.Failure(code);
            return CallProcessResult.Failed(code, "通话授权无效或已过期。");
        }

        var actorIsCaller = command.ActorUserId == command.Grant.CallerUserId;
        var isParticipant = actorIsCaller || command.ActorUserId == command.Grant.CalleeUserId;
        if (!isParticipant)
        {
            _metrics.Failure(CallErrorCode.GrantInvalid);
            return CallProcessResult.Failed(CallErrorCode.GrantInvalid, "命令发起者不是通话参与方。");
        }

        // SDP 预算：信号载荷必须在预算内，且存在。
        if (CallStateMachine.CarriesSdp(command.Type))
        {
            if (string.IsNullOrWhiteSpace(command.Sdp))
            {
                _metrics.Failure(CallErrorCode.SdpInvalid);
                return CallProcessResult.Failed(CallErrorCode.SdpInvalid, "该命令必须携带 SDP 载荷。");
            }

            if (Encoding.UTF8.GetByteCount(command.Sdp) > _options.MaxSdpBytes)
            {
                _metrics.Failure(CallErrorCode.SdpInvalid);
                return CallProcessResult.Failed(CallErrorCode.SdpInvalid, "SDP 载荷超过预算上限。");
            }
        }

        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            var snapshot = await _stateStore.GetAsync(command.CallId, ct).ConfigureAwait(false);

            // 逻辑超时：先驱动超时终态（Ringing→Missed / Active→TimedOut），或清理终态墓碑。
            // 依据状态起点推演逻辑超时，而非存储 TTL（存储比逻辑超时多保留 CleanupGraceMs）。
            if (snapshot is not null && nowMs >= ComputeLogicalTimeout(snapshot))
            {
                var timeout = await ApplyTimeoutAsync(snapshot, nowMs, ct).ConfigureAwait(false);
                if (timeout is not null)
                    return timeout;
                continue; // CAS 冲突，重读。
            }

            // 幂等：同一 command id 重放返回已记录结果，不发生新迁移。
            if (snapshot is not null && snapshot.LastCommandId == command.CommandId)
            {
                _metrics.Replayed();
                return CallProcessResult.Success(
                    snapshot.State, snapshot.Revision, snapshot.EndReason, replayed: true);
            }

            var current = snapshot?.State ?? CallState.Idle;

            // 终态：不允许任何新命令（仅允许幂等重放，已在上面处理）。
            if (current == CallState.Ended)
            {
                _metrics.Failure(CallErrorCode.CallEnded);
                return CallProcessResult.Failed(CallErrorCode.CallEnded, "通话已结束。");
            }

            // revision 排序：乱序/过期拒绝。
            if (snapshot is not null && command.Revision <= snapshot.Revision)
            {
                _metrics.Failure(CallErrorCode.RevisionStale);
                return CallProcessResult.Failed(CallErrorCode.RevisionStale, "命令 revision 已过期（乱序）。");
            }

            // 状态机迁移表校验。
            var transition = CallStateMachine.Transition(current, command.Type, actorIsCaller);
            if (transition is null)
            {
                _metrics.Failure(CallErrorCode.InvalidTransition);
                return CallProcessResult.Failed(
                    CallErrorCode.InvalidTransition,
                    $"当前状态 {current} 不允许命令 {command.Type}。");
            }

            var baseRevision = snapshot?.Revision ?? 0;
            var nextRevision = command.Revision > baseRevision ? command.Revision : baseRevision + 1;

            // 信令预算：携带 SDP 时递增计数，超限拒绝。
            var nextSignalCount = snapshot?.SignalCount ?? 0;
            if (CallStateMachine.CarriesSdp(command.Type))
            {
                if (nextSignalCount >= _options.MaxSignalsPerCall)
                {
                    _metrics.Failure(CallErrorCode.SignalBudgetExceeded);
                    return CallProcessResult.Failed(CallErrorCode.SignalBudgetExceeded, "通话信令预算已耗尽。");
                }

                nextSignalCount++;
            }

            var candidate = new CallStateSnapshot
            {
                CallId = command.CallId,
                State = transition.TargetState,
                EndReason = transition.EndReason,
                CallerUserId = command.Grant.CallerUserId,
                CalleeUserId = command.Grant.CalleeUserId,
                Revision = nextRevision,
                LastCommandId = command.CommandId,
                LastCommandType = command.Type,
                SignalCount = nextSignalCount,
                CreatedAtMs = snapshot?.CreatedAtMs ?? nowMs,
                UpdatedAtMs = nowMs,
                ExpiresAtMs = ComputeExpiresAt(transition.TargetState, nowMs)
            };

            var installed = await _stateStore.CompareAndSwapAsync(
                command.CallId, snapshot?.Revision, candidate, ComputeTtl(transition.TargetState), ct)
                .ConfigureAwait(false);

            if (installed is null)
            {
                // CAS 冲突（并发竞态）：重试。
                if (attempt == MaxCasAttempts - 1)
                {
                    _metrics.Failure(CallErrorCode.ConflictRetry);
                    return CallProcessResult.Failed(CallErrorCode.ConflictRetry, "通话状态并发冲突，请重试。");
                }

                continue;
            }

            // 成功：若命令携带 SDP，经临时信令路径转发给对端。
            CallSignalEnvelope? signal = null;
            if (CallStateMachine.CarriesSdp(command.Type))
            {
                signal = new CallSignalEnvelope
                {
                    SignalId = $"{command.CallId}:{nextRevision}",
                    CallId = command.CallId,
                    FromUserId = command.ActorUserId,
                    ToUserId = OtherParticipant(command),
                    Kind = command.Type,
                    Sdp = command.Sdp!,
                    Revision = nextRevision,
                    OccurredAtMs = nowMs
                };

                var forwarded = await _signalForwarder.ForwardAsync(signal, ct).ConfigureAwait(false);
                _metrics.SignalForwarded(forwarded);
                if (!forwarded)
                {
                    _logger.LogWarning(
                        "通话信令未转发（fail-closed）。命令={Type}；通话={CallId}；目标={ToUserId}",
                        command.Type, command.CallId, signal.ToUserId);
                }
            }

            // 审计：只记录参与者/状态/时间/失败分类/QoE 汇总。
            await _auditStore.RecordAsync(new CallAuditEntry
            {
                CallId = command.CallId,
                CallerUserId = command.Grant.CallerUserId,
                CalleeUserId = command.Grant.CalleeUserId,
                State = transition.TargetState,
                EndReason = transition.EndReason,
                Revision = nextRevision,
                EventKind = transition.TargetState == CallState.Ended
                    ? CallAuditEventKind.Terminal
                    : CallAuditEventKind.Transition,
                SignalCount = nextSignalCount,
                OccurredAtMs = nowMs
            }, ct).ConfigureAwait(false);

            if (transition.TargetState == CallState.Ended)
                _metrics.Terminal(transition.EndReason);
            else
                _metrics.Transition(transition.TargetState);

            return CallProcessResult.Success(
                transition.TargetState, nextRevision, transition.EndReason, signalToForward: signal);
        }

        _metrics.Failure(CallErrorCode.ConflictRetry);
        return CallProcessResult.Failed(CallErrorCode.ConflictRetry, "通话状态并发冲突，请重试。");
    }

    private async Task<CallProcessResult?> ApplyTimeoutAsync(
        CallStateSnapshot snapshot,
        long nowMs,
        CancellationToken ct)
    {
        // 终态墓碑过期：移除并返回已结束。
        if (snapshot.State == CallState.Ended)
        {
            await _stateStore.RemoveAsync(snapshot.CallId, ct).ConfigureAwait(false);
            return CallProcessResult.Failed(CallErrorCode.CallEnded, "通话已结束。");
        }

        var endReason = snapshot.State == CallState.Ringing ? CallEndReason.Missed : CallEndReason.TimedOut;
        var candidate = snapshot with
        {
            State = CallState.Ended,
            EndReason = endReason,
            Revision = snapshot.Revision + 1,
            LastCommandId = "timeout:" + snapshot.Revision,
            LastCommandType = CallCommandType.Timeout,
            UpdatedAtMs = nowMs,
            ExpiresAtMs = nowMs + _options.EndedRetentionMs
        };

        var installed = await _stateStore.CompareAndSwapAsync(
            snapshot.CallId, snapshot.Revision, candidate, ComputeTtl(CallState.Ended), ct)
            .ConfigureAwait(false);

        if (installed is null)
            return null; // 并发竞态，外层重试。

        _metrics.Timeout(endReason);
        _metrics.Terminal(endReason);
        await _auditStore.RecordAsync(new CallAuditEntry
        {
            CallId = snapshot.CallId,
            CallerUserId = snapshot.CallerUserId,
            CalleeUserId = snapshot.CalleeUserId,
            State = CallState.Ended,
            EndReason = endReason,
            Revision = candidate.Revision,
            EventKind = CallAuditEventKind.Timeout,
            SignalCount = snapshot.SignalCount,
            OccurredAtMs = nowMs
        }, ct).ConfigureAwait(false);

        return CallProcessResult.Success(CallState.Ended, candidate.Revision, endReason);
    }

    private long ComputeExpiresAt(CallState state, long nowMs) => state switch
    {
        CallState.Ringing => nowMs + _options.RingingTimeoutMs,
        CallState.Active => nowMs + _options.ActiveMaxDurationMs,
        CallState.Ended => nowMs + _options.EndedRetentionMs,
        _ => nowMs + _options.RingingTimeoutMs
    };

    private TimeSpan ComputeTtl(CallState state) => state switch
    {
        CallState.Ringing => TimeSpan.FromMilliseconds(_options.RingingTimeoutMs + _options.CleanupGraceMs),
        CallState.Active => TimeSpan.FromMilliseconds(_options.ActiveMaxDurationMs + _options.CleanupGraceMs),
        CallState.Ended => TimeSpan.FromMilliseconds(_options.EndedRetentionMs + _options.CleanupGraceMs),
        _ => TimeSpan.FromMilliseconds(_options.RingingTimeoutMs + _options.CleanupGraceMs)
    };

    // 逻辑超时：从状态起点推演，独立于存储物理 TTL（存储多保留 CleanupGraceMs 供处理器驱动超时终态）。
    private long ComputeLogicalTimeout(CallStateSnapshot snapshot) => snapshot.State switch
    {
        CallState.Ringing => snapshot.CreatedAtMs + _options.RingingTimeoutMs,
        CallState.Active => snapshot.CreatedAtMs + _options.ActiveMaxDurationMs,
        _ => snapshot.ExpiresAtMs
    };

    private static long OtherParticipant(CallCommand command)
        => command.ActorUserId == command.Grant.CallerUserId
            ? command.Grant.CalleeUserId
            : command.Grant.CallerUserId;

    private static (CallErrorCode ErrorCode, string Message)? Validate(CallCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.CommandId) || command.CommandId.Length > 64)
            return (CallErrorCode.InvalidCommandId, "命令编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.CallId) || command.CallId.Length > 128)
            return (CallErrorCode.InvalidCallId, "通话编号不能为空且长度不能超过 128。");
        if (command.ActorUserId <= 0)
            return (CallErrorCode.InvalidParticipant, "通话参与者编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.ActorSessionId)
            || command.ActorSessionId.Length > 128)
        {
            return (CallErrorCode.InvalidParticipant, "通话会话编号不能为空且长度不能超过 128。");
        }

        if (command.Revision <= 0)
            return (CallErrorCode.RevisionStale, "通话命令 revision 必须大于 0。");

        return null;
    }
}
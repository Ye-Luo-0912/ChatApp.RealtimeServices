using System.Text.Json;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

/// <summary>
/// 通话即时信令（SDP/ICE）转发器：经 Core NATS 零持久化 subject 转发给目标在线 Gateway。
/// <para>
/// 绝不进入 JetStream 流、PostgreSQL 或持久化 Outbox。转发前执行预算校验
/// （载荷大小、条数上限），任一超限即拒绝（fail-closed）。
/// </para>
/// </summary>
public sealed class NatsCallSignalForwarder : ICallSignalForwarder
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly CallPolicyOptions _policy;
    private readonly ILogger<NatsCallSignalForwarder> _logger;

    public NatsCallSignalForwarder(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        CallPolicyOptions policy,
        ILogger<NatsCallSignalForwarder> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _policy = policy;
        _logger = logger;
    }

    public async Task<bool> ForwardAsync(CallSignalEnvelope signal, CancellationToken ct = default)
    {
        // 预算校验（fail-closed）：SDP 载荷大小超限即拒绝。
        if (string.IsNullOrWhiteSpace(signal.Sdp)
            || System.Text.Encoding.UTF8.GetByteCount(signal.Sdp) > _policy.MaxSdpBytes)
        {
            _logger.LogWarning(
                "通话信令预算超限已拒绝。通话={CallId}；类型={Kind}；目标={ToUserId}",
                signal.CallId, signal.Kind, signal.ToUserId);
            return false;
        }

        var payload = JsonSerializer.Serialize(
            signal, RealtimeJsonSerializerContext.Default.CallSignalEnvelope);

        try
        {
            await _connectionClient.Client
                .PublishAsync(
                    _options.Topics.CallSignals,
                    payload,
                    headers: NatsTraceContext.CreatePropagationHeaders(),
                    cancellationToken: ct)
                .ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "通话信令转发失败（fail-closed）。通话={CallId}；类型={Kind}；目标={ToUserId}",
                signal.CallId, signal.Kind, signal.ToUserId);
            return false;
        }
    }
}
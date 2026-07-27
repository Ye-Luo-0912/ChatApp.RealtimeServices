using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.JetStream;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// 死信消息发布器：将处理失败的消息发布到死信 JetStream 流，便于后续对账与重放。
/// </summary>
internal sealed class NatsDeadLetterPublisher
{
    private readonly JetStreamTopologyManager _topology;
    private readonly RealtimeIntegrationOptions _options;

    public NatsDeadLetterPublisher(
        JetStreamTopologyManager topology,
        RealtimeIntegrationOptions options)
    {
        _topology = topology;
        _options = options;
    }

    /// <summary>
    /// 异步发布死信消息到死信流（不经重连重试，保证失败可见）。
    /// </summary>
    public async Task PublishDeadLetterAsync(DeadLetterMessage message, CancellationToken ct)
    {
        // Reliability-5：截断 payload 以适应 JetStream 1 MiB 单消息上限，记录 SHA-256 与原长度。
        var bounded = message.WithBoundedPayload();
        await _topology.EnsureStreamAsync(
            _options.DeadLettersStream,
            _options.DeadLettersSubject,
            _options.DeadLetterMaxAgeHours,
            ct).ConfigureAwait(false);
        await _topology.Context.PublishAsync(
            _options.DeadLettersSubject,
            RealtimeWireSerializer.Serialize(bounded),
            opts: JetStreamTopologyManager.BuildPublishOptions(bounded.DeadLetterId),
            cancellationToken: ct).ConfigureAwait(false);
    }
}

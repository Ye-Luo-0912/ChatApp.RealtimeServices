using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.JetStream;
using ChatApp.Realtime.Integration.Serialization;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// Realtime Event JetStream 持久化消费者：按 subject 与 durable consumer 名称订阅事件流，
/// 失败负载转死信，ACK/NAK 通过 <see cref="IntegrationJetStreamMetricAck"/> 记录指标。
/// </summary>
internal sealed class RealtimeEventSubscriber
{
    private readonly JetStreamTopologyManager _topology;
    private readonly NatsDeadLetterPublisher _deadLetterPublisher;
    private readonly NatsTransportMetrics _metrics;
    private readonly RealtimeIntegrationOptions _options;

    public RealtimeEventSubscriber(
        JetStreamTopologyManager topology,
        NatsDeadLetterPublisher deadLetterPublisher,
        NatsTransportMetrics metrics,
        RealtimeIntegrationOptions options)
    {
        _topology = topology;
        _deadLetterPublisher = deadLetterPublisher;
        _metrics = metrics;
        _options = options;
    }

    /// <summary>
    /// 是否启用按 Gateway 分片投递 Realtime Event。
    /// </summary>
    private bool IsShardedRoutingEnabled =>
        _options.RoutingMode is EventRoutingMode.Sharded
        && ShardedSubjectFormatter.IsSharded(_options.RealtimeEventsShardSubjectPattern);

    /// <summary>
    /// 当前实例订阅的 Realtime Event 分片 subject。
    /// </summary>
    private string RealtimeEventsShardSubject =>
        ShardedSubjectFormatter.Format(_options.RealtimeEventsShardSubjectPattern, _options.InstanceId);

    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
        CancellationToken ct = default)
    {
        // 分片模式：订阅本实例专属 subject；广播模式：订阅全量 subject。
        var subject = IsShardedRoutingEnabled
            ? RealtimeEventsShardSubject
            : _options.RealtimeEventsSubject;
        return ConsumeEventSubjectAsync(
            subject,
            CreateConsumerName(_options.GatewayConsumerPrefix, _options.InstanceId),
            ct);
    }

    public IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
        CancellationToken ct = default)
    {
        var consumerName = string.IsNullOrWhiteSpace(_options.AccountCleanupConsumerName)
            ? CreateConsumerName(_options.GatewayConsumerPrefix, _options.InstanceId)
            : NormalizeConsumerName(_options.AccountCleanupConsumerName);
        return ConsumeEventSubjectAsync(_options.AccountCleanupSubject, consumerName, ct);
    }

    private async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventSubjectAsync(
        string subject,
        string consumerName,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var stream = await _topology.EnsureStreamAsync(
            _options.RealtimeEventsStream,
            subject,
            _options.MaxAgeHours,
            ct).ConfigureAwait(false);
        var consumer = await stream.CreateOrUpdateConsumerAsync(
            new ConsumerConfig(consumerName)
            {
                FilterSubject = subject,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = TimeSpan.FromSeconds(_options.AckWaitSeconds),
                MaxDeliver = _options.MaxDeliver,
                MaxAckPending = _options.MaxAckPending,
                Backoff = _options.BackoffSeconds.Select(seconds => TimeSpan.FromSeconds(seconds)).ToArray(),
                DeliverPolicy = _options.ReplayRetainedEventsOnConsumerCreation
                    ? ConsumerConfigDeliverPolicy.All
                    : ConsumerConfigDeliverPolicy.New
            },
            ct).ConfigureAwait(false);
        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = Math.Max(1, _options.MaxAckPending),
            Expires = TimeSpan.FromSeconds(Math.Max(5, _options.AckWaitSeconds)),
            IdleHeartbeat = TimeSpan.FromSeconds(10),
            ThresholdMsgs = Math.Max(1, _options.MaxAckPending / 2)
        };

        while (!ct.IsCancellationRequested)
        {
            await foreach (var msg in consumer
                               .ConsumeAsync<string>(
                                   opts: consumeOptions,
                                   cancellationToken: ct)
                               .ConfigureAwait(false))
            {
                var observation = IntegrationJetStreamMetricAck.Observe(
                    _metrics,
                    msg.Metadata,
                    consumerName);
                RealtimeEvent? evt;
                try
                {
                    evt = string.IsNullOrWhiteSpace(msg.Data)
                        ? null
                        : RealtimeWireSerializer.DeserializeEvent(msg.Data);
                    if (evt is null)
                        throw new JsonException("实时事件负载为空或无法反序列化。");
                }
                catch (JsonException ex)
                {
                    await _deadLetterPublisher.PublishDeadLetterAsync(
                        new DeadLetterMessage
                        {
                            DeadLetterId = $"gateway-event-{msg.Metadata?.Sequence.Stream ?? 0}-invalid-json",
                            SourceSubject = msg.Subject,
                            ReasonCode = "invalid_event_json",
                            Reason = ex.Message,
                            Payload = msg.Data,
                            DeliveryCount = msg.Metadata?.NumDelivered
                        },
                        ct).ConfigureAwait(false);
                    await IntegrationJetStreamMetricAck.TerminateAsync(
                        msg,
                        _metrics,
                        observation,
                        "invalid_event_json",
                        ct).ConfigureAwait(false);
                    continue;
                }

                var jsMsg = msg;
                yield return new RealtimeEventDelivery(
                    evt,
                    ack: ackCt => IntegrationJetStreamMetricAck.AckAsync(
                        jsMsg, _metrics, observation, ackCt),
                    nak: (delay, nakCt) => IntegrationJetStreamMetricAck.NakAsync(
                        jsMsg, _metrics, observation, delay, nakCt),
                    deliveryCount: msg.Metadata?.NumDelivered,
                    parentContext: RealtimeIntegrationTelemetry.ExtractParentContext(msg.Headers));
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), ct)
                .ConfigureAwait(false);
        }
    }

    private static string CreateConsumerName(string prefix, string instanceId)
        => NormalizeConsumerName($"{prefix}-{instanceId}");

    private static string NormalizeConsumerName(string raw)
    {
        var normalized = new string(raw.Select(character =>
            char.IsLetterOrDigit(character) || character is '-' or '_' ? character : '-').ToArray());
        return normalized.Length <= 128 ? normalized : normalized[..128];
    }
}

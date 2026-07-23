using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly JetStreamContextManager _contextManager;
    private readonly JetStreamOptions _jetStreamOptions;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly NatsTransportMetrics _metrics;
    private readonly ILogger<JetStreamIncomingMessageConsumer> _logger;

    public JetStreamIncomingMessageConsumer(
        RealtimeQueueOptions options,
        RealtimeNatsTrustSettings trust,
        JetStreamContextManager contextManager,
        JetStreamOptions jetStreamOptions,
        IDeadLetterPublisher deadLetterPublisher,
        NatsTransportMetrics metrics,
        ILogger<JetStreamIncomingMessageConsumer> logger)
    {
        _options = options;
        _trust = trust;
        _contextManager = contextManager;
        _jetStreamOptions = jetStreamOptions;
        _deadLetterPublisher = deadLetterPublisher;
        _metrics = metrics;
        _logger = logger;
    }

    public async IAsyncEnumerable<IncomingMessageEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var consumer = await _contextManager
            .GetOrCreateIncomingMessagesConsumerAsync(ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "JetStream 入站消息消费者已启动。消费者={Consumer}；Subject={Subject}",
            _options.ConsumerGroup,
            _options.Topics.IncomingMessages);

        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = Math.Max(
                1,
                _jetStreamOptions.Consumer.MaxAckPending),
            Expires = TimeSpan.FromSeconds(Math.Max(
                5,
                _jetStreamOptions.Consumer.AckWaitSeconds)),
            IdleHeartbeat = TimeSpan.FromSeconds(10),
            ThresholdMsgs = Math.Max(
                1,
                _jetStreamOptions.Consumer.MaxAckPending / 2)
        };

        while (!ct.IsCancellationRequested)
        {
            await foreach (var msg in consumer
                               .ConsumeAsync<string>(
                                   opts: consumeOptions,
                                   cancellationToken: ct)
                               .ConfigureAwait(false))
            {
                var observation = JetStreamMetricAck.Observe(
                    _metrics,
                    msg.Metadata,
                    _options.ConsumerGroup);
                IncomingMessageCommand? command = null;

                try
                {
                    if (string.IsNullOrWhiteSpace(msg.Data))
                    {
                        await DeadLetterAndTerminateAsync(
                            msg,
                            "empty_payload",
                            "JetStream 入站消息为空。",
                            observation,
                            ct).ConfigureAwait(false);
                        continue;
                    }

                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.IncomingMessageCommand);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "JetStream 入站消息反序列化失败。Subject={Subject}",
                        msg.Subject);
                    await DeadLetterAndTerminateAsync(
                        msg,
                        "invalid_json",
                        ex.Message,
                        observation,
                        ct).ConfigureAwait(false);
                    continue;
                }

                if (command is not null)
                {
                    var jsMsg = msg;
                    var deliveryCount = jsMsg.Metadata?.NumDelivered;
                    var (trustedUserId, trustedSessionId) = NatsGatewayIdentity.Extract(
                        msg.Headers,
                        _trust.UserIdHeader,
                        _trust.SessionIdHeader);
                    yield return new IncomingMessageEnvelope(
                        command,
                        ack: ackCt => JetStreamMetricAck.AckAsync(
                            jsMsg, _metrics, observation, ackCt),
                        nak: (delay, nakCt) => JetStreamMetricAck.NakAsync(
                            jsMsg, _metrics, observation, delay, nakCt),
                        deliveryCount: deliveryCount,
                        rawPayload: msg.Data,
                        parentContext: NatsTraceContext.ExtractParentContext(msg.Headers),
                        trustedUserId: trustedUserId,
                        trustedSessionId: trustedSessionId);
                }
                else
                {
                    await JetStreamMetricAck.NakAsync(
                        msg, _metrics, observation, null, ct).ConfigureAwait(false);
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), ct)
                .ConfigureAwait(false);
        }
    }
    private async Task DeadLetterAndTerminateAsync(
        INatsJSMsg<string> msg,
        string reasonCode,
        string reason,
        JetStreamDeliveryObservation observation,
        CancellationToken ct)
    {
        var deadLetterId = $"incoming-{msg.Metadata?.Sequence.Stream ?? 0}-{reasonCode}";
        await _deadLetterPublisher.PublishAsync(
            new DeadLetterMessage
            {
                DeadLetterId = deadLetterId,
                SourceSubject = msg.Subject,
                ReasonCode = reasonCode,
                Reason = reason,
                Payload = msg.Data,
                DeliveryCount = msg.Metadata?.NumDelivered
            },
            ct).ConfigureAwait(false);
        await JetStreamMetricAck.TerminateAsync(
            msg, _metrics, observation, reasonCode, ct).ConfigureAwait(false);
    }
}

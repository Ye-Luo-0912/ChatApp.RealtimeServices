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

public sealed class JetStreamMessageReceiptConsumer : IMessageReceiptConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly JetStreamContextManager _contextManager;
    private readonly JetStreamOptions _jetStreamOptions;
    private readonly IDeadLetterPublisher _deadLetterPublisher;
    private readonly NatsTransportMetrics _metrics;
    private readonly ILogger<JetStreamMessageReceiptConsumer> _logger;

    public JetStreamMessageReceiptConsumer(
        RealtimeQueueOptions options,
        RealtimeNatsTrustSettings trust,
        JetStreamContextManager contextManager,
        JetStreamOptions jetStreamOptions,
        IDeadLetterPublisher deadLetterPublisher,
        NatsTransportMetrics metrics,
        ILogger<JetStreamMessageReceiptConsumer> logger)
    {
        _options = options;
        _trust = trust;
        _contextManager = contextManager;
        _jetStreamOptions = jetStreamOptions;
        _deadLetterPublisher = deadLetterPublisher;
        _metrics = metrics;
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageReceiptEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var consumer = await _contextManager
            .GetOrCreateMessageReceiptsConsumerAsync(ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "JetStream 消息回执消费者已启动。Subject={Subject}",
            _options.Topics.MessageReceipts);

        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = Math.Max(1, _jetStreamOptions.Consumer.MaxAckPending),
            Expires = TimeSpan.FromSeconds(
                Math.Max(5, _jetStreamOptions.Consumer.AckWaitSeconds)),
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
                var consumerName = $"{_options.ConsumerGroup}-receipts";
                var observation = JetStreamMetricAck.Observe(
                    _metrics,
                    msg.Metadata,
                    consumerName);
                MessageReceiptCommand? command = null;
                try
                {
                    if (string.IsNullOrWhiteSpace(msg.Data))
                    {
                        await DeadLetterAndTerminateAsync(
                            msg,
                            "empty_receipt_payload",
                            "JetStream 消息回执为空。",
                            observation,
                            ct).ConfigureAwait(false);
                        continue;
                    }

                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.MessageReceiptCommand);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(
                        ex,
                        "JetStream 消息回执反序列化失败。Subject={Subject}",
                        msg.Subject);
                    await DeadLetterAndTerminateAsync(
                        msg,
                        "invalid_receipt_json",
                        ex.Message,
                        observation,
                        ct).ConfigureAwait(false);
                    continue;
                }

                if (command is null)
                {
                    // P0-9：合法 JSON null 反序列化为 null，视为协议毒丸，
                    // 进入 DLQ 并 Terminate，不再 NAK 导致无限重投。
                    await DeadLetterAndTerminateAsync(
                        msg,
                        "null_receipt_payload",
                        "JetStream 消息回执反序列化结果为 null（合法 JSON null 或缺少必要字段）。",
                        observation,
                        ct).ConfigureAwait(false);
                    continue;
                }

                var jsMsg = msg;
                var (trustedUserId, _) = NatsGatewayIdentity.Extract(
                    msg.Headers,
                    _trust.UserIdHeader,
                    _trust.SessionIdHeader);
                yield return new MessageReceiptEnvelope(
                    command,
                    ack: ackCt => JetStreamMetricAck.AckAsync(
                        jsMsg, _metrics, observation, ackCt),
                    nak: (delay, nakCt) => JetStreamMetricAck.NakAsync(
                        jsMsg, _metrics, observation, delay, nakCt),
                    deliveryCount: msg.Metadata?.NumDelivered,
                    rawPayload: msg.Data,
                    parentContext: NatsTraceContext.ExtractParentContext(msg.Headers),
                    trustedUserId: trustedUserId);
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
        var deadLetterId =
            $"receipt-{msg.Metadata?.Sequence.Stream ?? 0}-{reasonCode}";
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
using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

/// <summary>
/// JetStream 账号清理事件消费者（durable = {QueueGroup}-account-cleanup）。
/// </summary>
public sealed class JetStreamRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly JetStreamContextManager _contextManager;
    private readonly JetStreamOptions _jetStreamOptions;
    private readonly ILogger<JetStreamRealtimeEventConsumer> _logger;

    public JetStreamRealtimeEventConsumer(
        RealtimeQueueOptions options,
        JetStreamContextManager contextManager,
        JetStreamOptions jetStreamOptions,
        ILogger<JetStreamRealtimeEventConsumer> logger)
    {
        _options = options;
        _contextManager = contextManager;
        _jetStreamOptions = jetStreamOptions;
        _logger = logger;
    }

    public async IAsyncEnumerable<RealtimeEventEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var consumer = await _contextManager
            .GetOrCreateAccountCleanupConsumerAsync(ct)
            .ConfigureAwait(false);

        var durable = $"{_options.ConsumerGroup}-account-cleanup";
        _logger.LogInformation(
            "JetStream 账号清理消费者已启动。消费者={Consumer}；Subject={Subject}",
            durable,
            _options.Topics.AccountCleanup);

        // Reliability-4：使用 PrefetchMaxMsgs 而非默认 MaxAckPending，避免本地队列堆积消耗 AckWait。
        var consumeOptions = new NatsJSConsumeOpts
        {
            MaxMsgs = Math.Max(1, _jetStreamOptions.Consumer.PrefetchMaxMsgs),
            Expires = TimeSpan.FromSeconds(
                Math.Max(5, _jetStreamOptions.Consumer.AckWaitSeconds)),
            IdleHeartbeat = TimeSpan.FromSeconds(10),
            ThresholdMsgs = Math.Max(
                1,
                _jetStreamOptions.Consumer.PrefetchMaxMsgs / 2)
        };

        await foreach (var msg in consumer.ConsumeAsync<string>(
                           opts: consumeOptions,
                           cancellationToken: ct))
        {
            RealtimeEvent? evt = null;
            try
            {
                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("JetStream 实时事件为空，已终止。Subject={Subject}", msg.Subject);
                    await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                    continue;
                }

                evt = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.RealtimeEvent);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "JetStream 实时事件反序列化失败，已终止。Subject={Subject}", msg.Subject);
                await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            if (evt is null)
            {
                await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            var jsMsg = msg;
            yield return new RealtimeEventEnvelope(
                evt,
                ack: ackCt => new ValueTask(jsMsg.AckAsync(cancellationToken: ackCt).AsTask()),
                nak: nakCt => new ValueTask(jsMsg.NakAsync(cancellationToken: nakCt).AsTask()),
                deliveryCount: jsMsg.Metadata?.NumDelivered);
        }
    }
}

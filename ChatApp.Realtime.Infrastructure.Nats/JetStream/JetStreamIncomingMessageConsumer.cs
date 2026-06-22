using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly JetStreamContextManager _contextManager;
    private readonly ILogger<JetStreamIncomingMessageConsumer> _logger;

    public JetStreamIncomingMessageConsumer(
        RealtimeQueueOptions options,
        JetStreamContextManager contextManager,
        ILogger<JetStreamIncomingMessageConsumer> logger)
    {
        _options = options;
        _contextManager = contextManager;
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

        await foreach (var msg in consumer.ConsumeAsync<string>(cancellationToken: ct))
        {
            IncomingMessageCommand? command = null;

            try
            {
                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("JetStream 入站消息为空，已跳过。Subject={Subject}", msg.Subject);
                    await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
                    continue;
                }

                command = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.IncomingMessageCommand);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "JetStream 入站消息反序列化失败。Subject={Subject}", msg.Subject);
                await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            if (command is not null)
            {
                var jsMsg = msg;
                var deliveryCount = jsMsg.Metadata?.NumDelivered;
                yield return new IncomingMessageEnvelope(
                    command,
                    ack: async ackCt => await jsMsg.AckAsync(cancellationToken: ackCt).ConfigureAwait(false),
                    nak: async nakCt => await jsMsg.NakAsync(cancellationToken: nakCt).ConfigureAwait(false),
                    deliveryCount: deliveryCount);
            }
            else
            {
                await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
            }
        }
    }
}

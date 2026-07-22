using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsMessageReceiptConsumer : IMessageReceiptConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsMessageReceiptConsumer> _logger;

    public NatsMessageReceiptConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsMessageReceiptConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageReceiptEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 消息回执消费者已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.MessageReceipts,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.MessageReceipts,
                           $"{_options.ConsumerGroup}-receipts",
                           cancellationToken: ct))
        {
            MessageReceiptCommand? command = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.MessageReceiptCommand);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "NATS 消息回执读取失败。Subject={Subject}",
                    msg.Subject);
            }

            if (command is not null)
            {
                yield return new MessageReceiptEnvelope(command)
                {
                    RawPayload = msg.Data,
                    ParentContext = NatsTraceContext.ExtractParentContext(msg.Headers)
                };
            }
        }
    }
}
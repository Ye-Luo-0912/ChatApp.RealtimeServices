using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsIncomingMessageConsumer> _logger;

    public NatsIncomingMessageConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsIncomingMessageConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<IncomingMessageCommand> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 入站消息消费者已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.IncomingMessages,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.IncomingMessages,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            IncomingMessageCommand? command = null;

            try
            {
                msg.EnsureSuccess();

                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("NATS 入站消息为空，已跳过。Subject={Subject}", msg.Subject);
                    continue;
                }

                command = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.IncomingMessageCommand);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "NATS 入站消息反序列化失败。Subject={Subject}", msg.Subject);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NATS 入站消息读取失败。Subject={Subject}", msg.Subject);
            }

            if (command is not null)
            {
                yield return command;
            }
        }
    }
}

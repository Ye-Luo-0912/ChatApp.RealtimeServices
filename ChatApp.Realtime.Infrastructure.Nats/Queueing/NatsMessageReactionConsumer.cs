using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsMessageReactionConsumer : IMessageReactionConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsMessageReactionConsumer> _logger;

    public NatsMessageReactionConsumer(
        RealtimeQueueOptions options,
        RealtimeNatsTrustSettings trust,
        NatsConnectionClient connectionClient,
        ILogger<NatsMessageReactionConsumer> logger)
    {
        _options = options;
        _trust = trust;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageReactionEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 消息反应端点已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.MessageReactions,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.MessageReactions,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            MessageReactionCommand? command = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.MessageReactionCommand);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "NATS 消息反应命令反序列化失败。Subject={Subject}",
                    msg.Subject);
            }

            if (command is null)
            {
                var invalid = MessageReactionResult.Failed(
                    string.Empty,
                    "invalid_command_json",
                    "消息反应命令负载为空或格式无效。");
                var invalidJson = JsonSerializer.Serialize(
                    invalid,
                    RealtimeJsonSerializerContext.Default.MessageReactionResult);
                await msg.ReplyAsync(invalidJson, cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            var (trustedUserId, _) = NatsGatewayIdentity.Extract(
                msg.Headers,
                _trust.UserIdHeader,
                _trust.SessionIdHeader);
            yield return new MessageReactionEnvelope(
                command,
                async (result, replyCt) =>
                {
                    var json = JsonSerializer.Serialize(
                        result,
                        RealtimeJsonSerializerContext.Default.MessageReactionResult);
                    await msg.ReplyAsync(json, cancellationToken: replyCt).ConfigureAwait(false);
                },
                parentContext: NatsTraceContext.ExtractParentContext(msg.Headers),
                trustedUserId: trustedUserId);
        }
    }
}

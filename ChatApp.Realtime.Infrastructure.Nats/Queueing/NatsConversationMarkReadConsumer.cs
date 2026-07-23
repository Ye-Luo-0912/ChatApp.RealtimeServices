using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsConversationMarkReadConsumer : IConversationMarkReadConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsConversationMarkReadConsumer> _logger;

    public NatsConversationMarkReadConsumer(
        RealtimeQueueOptions options,
        RealtimeNatsTrustSettings trust,
        NatsConnectionClient connectionClient,
        ILogger<NatsConversationMarkReadConsumer> logger)
    {
        _options = options;
        _trust = trust;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<ConversationMarkReadEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 会话已读推进端点已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.ConversationMarkReads,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.ConversationMarkReads,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            ConversationMarkReadCommand? command = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    command = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.ConversationMarkReadCommand);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "NATS 会话已读命令反序列化失败。Subject={Subject}",
                    msg.Subject);
            }

            if (command is null)
            {
                var invalid = ConversationMarkReadResult.Failed(
                    string.Empty,
                    "invalid_command_json",
                    "会话已读命令负载为空或格式无效。");
                var invalidJson = JsonSerializer.Serialize(
                    invalid,
                    RealtimeJsonSerializerContext.Default.ConversationMarkReadResult);
                await msg.ReplyAsync(invalidJson, cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            var (trustedUserId, _) = NatsGatewayIdentity.Extract(
                msg.Headers,
                _trust.UserIdHeader,
                _trust.SessionIdHeader);
            yield return new ConversationMarkReadEnvelope(
                command,
                async (result, replyCt) =>
                {
                    var json = JsonSerializer.Serialize(
                        result,
                        RealtimeJsonSerializerContext.Default.ConversationMarkReadResult);
                    await msg.ReplyAsync(json, cancellationToken: replyCt).ConfigureAwait(false);
                },
                parentContext: NatsTraceContext.ExtractParentContext(msg.Headers),
                trustedUserId: trustedUserId);
        }
    }
}

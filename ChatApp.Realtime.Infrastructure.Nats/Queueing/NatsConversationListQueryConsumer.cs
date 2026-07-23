using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsConversationListQueryConsumer : IConversationListQueryConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly RealtimeNatsTrustSettings _trust;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsConversationListQueryConsumer> _logger;

    public NatsConversationListQueryConsumer(
        RealtimeQueueOptions options,
        RealtimeNatsTrustSettings trust,
        NatsConnectionClient connectionClient,
        ILogger<NatsConversationListQueryConsumer> logger)
    {
        _options = options;
        _trust = trust;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<ConversationListQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 会话列表查询端点已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.ConversationListQueries,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.ConversationListQueries,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            ConversationListQuery? query = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    query = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.ConversationListQuery);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "NATS 会话列表查询反序列化失败。Subject={Subject}",
                    msg.Subject);
            }

            if (query is null)
            {
                var invalidPage = ConversationListPage.Failed(
                    string.Empty,
                    "invalid_query_json",
                    "会话列表查询负载为空或格式无效。");
                var invalidJson = JsonSerializer.Serialize(
                    invalidPage,
                    RealtimeJsonSerializerContext.Default.ConversationListPage);
                await msg.ReplyAsync(invalidJson, cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            var (trustedUserId, _) = NatsGatewayIdentity.Extract(
                msg.Headers,
                _trust.UserIdHeader,
                _trust.SessionIdHeader);
            yield return new ConversationListQueryEnvelope(
                query,
                async (page, replyCt) =>
                {
                    var json = JsonSerializer.Serialize(
                        page,
                        RealtimeJsonSerializerContext.Default.ConversationListPage);
                    await msg.ReplyAsync(json, cancellationToken: replyCt).ConfigureAwait(false);
                },
                parentContext: NatsTraceContext.ExtractParentContext(msg.Headers),
                trustedUserId: trustedUserId);
        }
    }
}

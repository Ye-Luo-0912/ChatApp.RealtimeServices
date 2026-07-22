using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsMessageHistoryQueryConsumer : IMessageHistoryQueryConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsMessageHistoryQueryConsumer> _logger;

    public NatsMessageHistoryQueryConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsMessageHistoryQueryConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageHistoryQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 历史消息查询端点已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.MessageHistoryQueries,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.MessageHistoryQueries,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            MessageHistoryQuery? query = null;
            try
            {
                msg.EnsureSuccess();
                if (!string.IsNullOrWhiteSpace(msg.Data))
                {
                    query = JsonSerializer.Deserialize(
                        msg.Data,
                        RealtimeJsonSerializerContext.Default.MessageHistoryQuery);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(
                    ex,
                    "NATS 历史消息查询反序列化失败。Subject={Subject}",
                    msg.Subject);
            }

            if (query is null)
            {
                var invalidPage = MessageHistoryPage.Failed(
                    string.Empty,
                    "invalid_query_json",
                    "历史消息查询负载为空或格式无效。");
                var invalidJson = JsonSerializer.Serialize(
                    invalidPage,
                    RealtimeJsonSerializerContext.Default.MessageHistoryPage);
                await msg.ReplyAsync(
                        invalidJson,
                        cancellationToken: ct)
                    .ConfigureAwait(false);
                continue;
            }

            yield return new MessageHistoryQueryEnvelope(
                query,
                async (page, replyCt) =>
                {
                    var json = JsonSerializer.Serialize(
                        page,
                        RealtimeJsonSerializerContext.Default.MessageHistoryPage);
                    await msg.ReplyAsync(
                            json,
                            cancellationToken: replyCt)
                        .ConfigureAwait(false);
                },
                parentContext: NatsTraceContext.ExtractParentContext(msg.Headers));
        }
    }
}
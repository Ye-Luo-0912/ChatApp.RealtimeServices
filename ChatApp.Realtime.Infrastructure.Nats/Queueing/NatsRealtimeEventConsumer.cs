using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsRealtimeEventConsumer> _logger;

    public NatsRealtimeEventConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async IAsyncEnumerable<RealtimeEventEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Core NATS 队列组：专用 cleanup 队列名，避免与网关推送抢同一订阅组。
        var queueGroup = $"{_options.ConsumerGroup}-account-cleanup";
        _logger.LogInformation(
            "NATS 账号清理事件消费者已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.RealtimeEvents,
            queueGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.RealtimeEvents,
                           queueGroup,
                           cancellationToken: ct))
        {
            RealtimeEvent? evt = null;
            try
            {
                msg.EnsureSuccess();
                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("NATS 实时事件为空，已跳过。Subject={Subject}", msg.Subject);
                    continue;
                }

                evt = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.RealtimeEvent);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "NATS 实时事件反序列化失败。Subject={Subject}", msg.Subject);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NATS 实时事件读取失败。Subject={Subject}", msg.Subject);
            }

            if (evt is not null)
                yield return new RealtimeEventEnvelope(evt);
        }
    }
}

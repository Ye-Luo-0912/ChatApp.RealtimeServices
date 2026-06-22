using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly JetStreamContextManager _contextManager;
    private readonly ILogger<JetStreamRealtimeEventConsumer> _logger;

    public JetStreamRealtimeEventConsumer(
        RealtimeQueueOptions options,
        JetStreamContextManager contextManager,
        ILogger<JetStreamRealtimeEventConsumer> logger)
    {
        _options = options;
        _contextManager = contextManager;
        _logger = logger;
    }

    public async IAsyncEnumerable<RealtimeEvent> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var consumer = await _contextManager
            .GetOrCreateRealtimeEventsConsumerAsync(ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "JetStream 实时事件消费者已启动。消费者={Consumer}；Subject={Subject}",
            _options.ConsumerGroup,
            _options.Topics.RealtimeEvents);

        INatsJSMsg<string>? previousMsg = null;

        await foreach (var msg in consumer.ConsumeAsync<string>(cancellationToken: ct))
        {
            if (previousMsg is not null)
            {
                await previousMsg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                previousMsg = null;
            }

            RealtimeEvent? evt = null;

            try
            {
                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("JetStream 实时事件为空，已跳过。Subject={Subject}", msg.Subject);
                    await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
                    continue;
                }

                evt = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.RealtimeEvent);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "JetStream 实时事件反序列化失败。Subject={Subject}", msg.Subject);
                await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            if (evt is not null)
            {
                previousMsg = msg;
                yield return evt;
            }
            else
            {
                await msg.NakAsync(cancellationToken: ct).ConfigureAwait(false);
            }
        }
    }
}

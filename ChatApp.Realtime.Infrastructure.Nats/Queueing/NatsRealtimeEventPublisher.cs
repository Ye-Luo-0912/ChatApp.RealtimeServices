using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsRealtimeEventPublisher> _logger;

    public NatsRealtimeEventPublisher(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventPublisher> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(
            evt,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);

        await _connectionClient.Client
            .PublishAsync(_options.Topics.RealtimeEvents, json, cancellationToken: ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "实时事件已发布到 NATS。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}；Subject={Subject}",
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            _options.Topics.RealtimeEvents);
    }
}

using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
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

        var subject = evt.Type is RealtimeEventType.UserAccountDeleted or RealtimeEventType.AccountCleanupCompleted
            ? _options.Topics.AccountCleanup
            : _options.Topics.RealtimeEvents;

        await _connectionClient.Client
            .PublishAsync(subject, json, headers: NatsTraceContext.CreatePropagationHeaders(), cancellationToken: ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "实时事件已发布到 NATS。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}；Subject={Subject}",
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            subject);
    }
}

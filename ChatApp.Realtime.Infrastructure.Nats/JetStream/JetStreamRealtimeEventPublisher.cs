using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly RealtimeQueueOptions _options;
    private readonly JetStreamContextManager _contextManager;
    private readonly ILogger<JetStreamRealtimeEventPublisher> _logger;

    public JetStreamRealtimeEventPublisher(
        RealtimeQueueOptions options,
        JetStreamContextManager contextManager,
        ILogger<JetStreamRealtimeEventPublisher> logger)
    {
        _options = options;
        _contextManager = contextManager;
        _logger = logger;
    }

    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(
            evt,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);

        var ack = await _contextManager.Context
            .PublishAsync(_options.Topics.RealtimeEvents, json, cancellationToken: ct)
            .ConfigureAwait(false);

        if (ack.Error is not null)
        {
            _logger.LogError(
                "JetStream 实时事件发布失败。事件编号={EventId}；Subject={Subject}；错误={Error}",
                evt.EventId,
                _options.Topics.RealtimeEvents,
                ack.Error.Description);
            throw new InvalidOperationException($"JetStream 发布未确认。事件编号={evt.EventId}");
        }

        _logger.LogInformation(
            "实时事件已发布到 JetStream。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}；Subject={Subject}；序列号={Seq}",
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            _options.Topics.RealtimeEvents,
            ack.Seq);
    }
}

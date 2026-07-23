using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly JetStreamContextManager _contextManager;

    public JetStreamRealtimeEventPublisher(JetStreamContextManager contextManager)
    {
        _contextManager = contextManager;
    }

    public Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);
        // 账号清理相关事件走专用 subject，避免清理 durable 消费/ACK 网关噪声。
        if (evt.Type is RealtimeEventType.UserAccountDeleted
            or RealtimeEventType.AccountCleanupCompleted
            or RealtimeEventType.AttachmentBlobsPurge)
        {
            return _contextManager.PublishAccountCleanupEventAsync(evt.EventId, payload, ct);
        }

        return _contextManager.PublishRealtimeEventAsync(evt.EventId, payload, ct);
    }
}

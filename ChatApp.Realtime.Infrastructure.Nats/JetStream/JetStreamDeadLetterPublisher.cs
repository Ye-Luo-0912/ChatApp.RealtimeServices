using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamDeadLetterPublisher : IDeadLetterPublisher
{
    private readonly JetStreamContextManager _contextManager;

    public JetStreamDeadLetterPublisher(JetStreamContextManager contextManager)
    {
        _contextManager = contextManager;
    }

    public Task PublishAsync(DeadLetterMessage message, CancellationToken ct = default)
    {
        // Reliability-5：截断 payload 以适应 JetStream 1 MiB 单消息上限，记录 SHA-256 与原长度。
        var bounded = message.WithBoundedPayload();
        return _contextManager.PublishDeadLetterAsync(
            bounded.DeadLetterId,
            JsonSerializer.Serialize(bounded, RealtimeJsonSerializerContext.Default.DeadLetterMessage),
            ct);
    }
}

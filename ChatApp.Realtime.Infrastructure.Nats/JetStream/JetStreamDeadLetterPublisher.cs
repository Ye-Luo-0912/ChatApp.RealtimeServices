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

    public Task PublishAsync(DeadLetterMessage message, CancellationToken ct = default) =>
        _contextManager.PublishDeadLetterAsync(
            message.DeadLetterId,
            JsonSerializer.Serialize(message, RealtimeJsonSerializerContext.Default.DeadLetterMessage),
            ct);
}

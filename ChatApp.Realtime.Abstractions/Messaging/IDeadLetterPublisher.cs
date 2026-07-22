namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IDeadLetterPublisher
{
    Task PublishAsync(DeadLetterMessage message, CancellationToken ct = default);
}

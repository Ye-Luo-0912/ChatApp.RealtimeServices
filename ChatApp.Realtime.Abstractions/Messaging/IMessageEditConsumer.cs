namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageEditConsumer
{
    IAsyncEnumerable<MessageEditEnvelope> ConsumeAsync(CancellationToken ct = default);
}

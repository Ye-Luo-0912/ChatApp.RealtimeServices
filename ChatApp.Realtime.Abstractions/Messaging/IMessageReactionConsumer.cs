namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageReactionConsumer
{
    IAsyncEnumerable<MessageReactionEnvelope> ConsumeAsync(CancellationToken ct = default);
}

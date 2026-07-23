namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageRecallConsumer
{
    IAsyncEnumerable<MessageRecallEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

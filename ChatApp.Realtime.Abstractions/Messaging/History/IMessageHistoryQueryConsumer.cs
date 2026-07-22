namespace ChatApp.Realtime.Abstractions.Messaging.History;

public interface IMessageHistoryQueryConsumer
{
    IAsyncEnumerable<MessageHistoryQueryEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

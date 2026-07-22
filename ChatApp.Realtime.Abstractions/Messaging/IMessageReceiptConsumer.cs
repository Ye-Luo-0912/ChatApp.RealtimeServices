namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageReceiptConsumer
{
    IAsyncEnumerable<MessageReceiptEnvelope> ConsumeAsync(CancellationToken ct = default);
}
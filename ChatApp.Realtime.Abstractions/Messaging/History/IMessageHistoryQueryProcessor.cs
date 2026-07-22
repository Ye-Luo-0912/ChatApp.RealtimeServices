namespace ChatApp.Realtime.Abstractions.Messaging.History;

public interface IMessageHistoryQueryProcessor
{
    Task<MessageHistoryPage> ProcessAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default);
}

namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationListQueryConsumer
{
    IAsyncEnumerable<ConversationListQueryEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

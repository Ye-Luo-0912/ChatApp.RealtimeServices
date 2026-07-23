namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationMarkReadConsumer
{
    IAsyncEnumerable<ConversationMarkReadEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

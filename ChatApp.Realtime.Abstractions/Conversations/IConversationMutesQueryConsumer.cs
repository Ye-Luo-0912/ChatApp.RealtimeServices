namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationMutesQueryConsumer
{
    IAsyncEnumerable<ConversationMutesQueryEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

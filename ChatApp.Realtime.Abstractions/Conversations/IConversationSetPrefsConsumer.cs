namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationSetPrefsConsumer
{
    IAsyncEnumerable<ConversationSetPrefsEnvelope> ConsumeAsync(
        CancellationToken ct = default);
}

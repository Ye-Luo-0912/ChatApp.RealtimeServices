namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationSetPrefsProcessor
{
    Task<ConversationSetPrefsResult> ProcessAsync(
        ConversationSetPrefsCommand command,
        CancellationToken ct = default);
}

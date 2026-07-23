namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationMarkReadProcessor
{
    Task<ConversationMarkReadResult> ProcessAsync(
        ConversationMarkReadCommand command,
        CancellationToken ct = default);
}

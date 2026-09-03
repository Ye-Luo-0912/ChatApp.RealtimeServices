namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationMutesQueryProcessor
{
    Task<ConversationMutesQueryResult> ProcessAsync(
        ConversationMutesQuery query,
        CancellationToken ct = default);
}

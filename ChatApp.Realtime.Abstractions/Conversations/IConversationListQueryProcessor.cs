namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IConversationListQueryProcessor
{
    Task<ConversationListPage> ProcessAsync(
        ConversationListQuery query,
        CancellationToken ct = default);
}

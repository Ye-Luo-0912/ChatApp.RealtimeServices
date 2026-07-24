namespace ChatApp.Realtime.Abstractions.Conversations;

public interface IGroupConversationProcessor
{
    Task<GroupConversationResult> ProcessAsync(
        GroupConversationCommand command,
        CancellationToken ct = default);
}

public interface IGroupConversationConsumer
{
    IAsyncEnumerable<GroupConversationEnvelope> ConsumeAsync(CancellationToken ct = default);
}

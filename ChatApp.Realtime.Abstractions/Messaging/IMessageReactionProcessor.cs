namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageReactionProcessor
{
    Task<MessageReactionResult> ProcessAsync(
        MessageReactionCommand command,
        CancellationToken ct = default);
}

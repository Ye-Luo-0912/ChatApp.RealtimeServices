namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageEditProcessor
{
    Task<MessageEditResult> ProcessAsync(
        MessageEditCommand command,
        CancellationToken ct = default);
}

namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageRecallProcessor
{
    Task<MessageRecallResult> ProcessAsync(
        MessageRecallCommand command,
        CancellationToken ct = default);
}

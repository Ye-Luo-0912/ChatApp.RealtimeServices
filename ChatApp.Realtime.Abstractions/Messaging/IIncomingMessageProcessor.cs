namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IIncomingMessageProcessor
{
    Task<MessageProcessResult> ProcessAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default);
}

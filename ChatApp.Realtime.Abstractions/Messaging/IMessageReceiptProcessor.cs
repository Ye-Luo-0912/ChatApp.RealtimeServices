namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IMessageReceiptProcessor
{
    Task<MessageProcessResult> ProcessAsync(
        MessageReceiptCommand command,
        CancellationToken ct = default);
}
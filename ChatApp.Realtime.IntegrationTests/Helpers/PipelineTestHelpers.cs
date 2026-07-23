using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;

namespace ChatApp.Realtime.IntegrationTests.Helpers;

internal static class PipelineTestIds
{
    public static string CreateMessageCommandId(long senderUserId, string clientMessageId)
    {
        var source = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(source));
    }

    public static string CreateReceiptCommandId(
        long receiverUserId,
        string messageId,
        MessageReceiptType receiptType)
    {
        var source = Encoding.UTF8.GetBytes(
            $"{receiverUserId}:{messageId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(SHA256.HashData(source));
    }
}

internal static class EventWaiter
{
    public static async Task<RealtimeEvent> WaitForAsync(
        IRealtimeMessageBus messageBus,
        string messageId,
        RealtimeEventType eventType,
        CancellationToken cancellationToken)
    {
        await foreach (var delivery in messageBus
                           .ConsumeEventsAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            await delivery.AckAsync(cancellationToken).ConfigureAwait(false);

            if (delivery.Event.Type == eventType &&
                string.Equals(
                    delivery.Event.MessageId,
                    messageId,
                    StringComparison.Ordinal))
            {
                return delivery.Event;
            }
        }

        throw new InvalidOperationException(
            $"Timed out waiting for {eventType} event for message {messageId}.");
    }
}

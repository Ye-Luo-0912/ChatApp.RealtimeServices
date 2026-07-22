using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Integration.Serialization;

public static class RealtimeWireSerializer
{
    public static string Serialize(IncomingMessageCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.IncomingMessageCommand);

    public static string Serialize(MessageReceiptCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageReceiptCommand);

    public static string Serialize(MessageHistoryQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.MessageHistoryQuery);

    public static string Serialize(RealtimeEvent evt) =>
        JsonSerializer.Serialize(evt, RealtimeIntegrationJsonContext.Default.RealtimeEvent);

    public static string Serialize(DeadLetterMessage message) =>
        JsonSerializer.Serialize(message, RealtimeIntegrationJsonContext.Default.DeadLetterMessage);

    public static MessageHistoryPage? DeserializeMessageHistoryPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageHistoryPage);

    public static RealtimeEvent? DeserializeEvent(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeEvent);

    public static RealtimeChatMessagePayload? DeserializeChatMessage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeChatMessagePayload);

    public static RealtimeMessageReceiptPayload? DeserializeMessageReceipt(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMessageReceiptPayload);

    public static string Serialize(RealtimeDomainNotificationPayload payload) =>
        JsonSerializer.Serialize(payload, RealtimeIntegrationJsonContext.Default.RealtimeDomainNotificationPayload);
}

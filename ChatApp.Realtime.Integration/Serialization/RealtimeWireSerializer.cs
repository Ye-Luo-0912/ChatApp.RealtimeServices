using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Integration.Serialization;

public static class RealtimeWireSerializer
{
    public static string Serialize(IncomingMessageCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.IncomingMessageCommand);

    public static string Serialize(MessageReceiptCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageReceiptCommand);

    public static string Serialize(MessageHistoryQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.MessageHistoryQuery);

    public static string Serialize(ConversationListQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.ConversationListQuery);

    public static string Serialize(ConversationMarkReadCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.ConversationMarkReadCommand);

    public static string Serialize(ConversationSetPrefsCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.ConversationSetPrefsCommand);

    public static string Serialize(MessageRecallCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageRecallCommand);

    public static string Serialize(SyncBootstrapQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.SyncBootstrapQuery);

    public static string Serialize(RealtimeEvent evt) =>
        JsonSerializer.Serialize(evt, RealtimeIntegrationJsonContext.Default.RealtimeEvent);

    public static string Serialize(DeadLetterMessage message) =>
        JsonSerializer.Serialize(message, RealtimeIntegrationJsonContext.Default.DeadLetterMessage);

    public static MessageHistoryPage? DeserializeMessageHistoryPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageHistoryPage);

    public static ConversationListPage? DeserializeConversationListPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationListPage);

    public static ConversationMarkReadResult? DeserializeConversationMarkReadResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationMarkReadResult);

    public static ConversationSetPrefsResult? DeserializeConversationSetPrefsResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationSetPrefsResult);

    public static MessageRecallResult? DeserializeMessageRecallResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageRecallResult);

    public static SyncBootstrapPage? DeserializeSyncBootstrapPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.SyncBootstrapPage);

    public static RealtimeEvent? DeserializeEvent(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeEvent);

    public static RealtimeChatMessagePayload? DeserializeChatMessage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeChatMessagePayload);

    public static RealtimeConversationChangedPayload? DeserializeConversationChanged(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeConversationChangedPayload);

    public static RealtimeUnreadCountChangedPayload? DeserializeUnreadCountChanged(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeUnreadCountChangedPayload);

    public static RealtimeMessageReceiptPayload? DeserializeMessageReceipt(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMessageReceiptPayload);

    public static RealtimeMessageRecalledPayload? DeserializeMessageRecalled(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMessageRecalledPayload);

    public static string Serialize(RealtimeDomainNotificationPayload payload) =>
        JsonSerializer.Serialize(payload, RealtimeIntegrationJsonContext.Default.RealtimeDomainNotificationPayload);
}

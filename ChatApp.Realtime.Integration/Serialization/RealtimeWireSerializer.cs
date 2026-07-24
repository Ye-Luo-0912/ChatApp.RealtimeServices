using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration.Ephemeral;

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

    public static string Serialize(GroupConversationCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.GroupConversationCommand);

    public static string Serialize(MessageRecallCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageRecallCommand);

    public static string Serialize(MessageEditCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageEditCommand);

    public static string Serialize(MessageReactionCommand command) =>
        JsonSerializer.Serialize(command, RealtimeIntegrationJsonContext.Default.MessageReactionCommand);

    public static string Serialize(SyncBootstrapQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.SyncBootstrapQuery);

    public static string Serialize(RealtimeEvent evt) =>
        JsonSerializer.Serialize(evt, RealtimeIntegrationJsonContext.Default.RealtimeEvent);

    public static string Serialize(DeadLetterMessage message) =>
        JsonSerializer.Serialize(message, RealtimeIntegrationJsonContext.Default.DeadLetterMessage);

    public static string Serialize(EphemeralTypingEvent evt) =>
        JsonSerializer.Serialize(evt, RealtimeIntegrationJsonContext.Default.EphemeralTypingEvent);

    public static string Serialize(EphemeralPresenceEvent evt) =>
        JsonSerializer.Serialize(evt, RealtimeIntegrationJsonContext.Default.EphemeralPresenceEvent);

    public static string Serialize(PresenceAuthorizeQuery query) =>
        JsonSerializer.Serialize(query, RealtimeIntegrationJsonContext.Default.PresenceAuthorizeQuery);

    public static string Serialize(PresenceAuthorizeResponse response) =>
        JsonSerializer.Serialize(response, RealtimeIntegrationJsonContext.Default.PresenceAuthorizeResponse);

    public static EphemeralTypingEvent? DeserializeEphemeralTyping(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.EphemeralTypingEvent);

    public static EphemeralPresenceEvent? DeserializeEphemeralPresence(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.EphemeralPresenceEvent);

    public static PresenceAuthorizeQuery? DeserializePresenceAuthorizeQuery(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.PresenceAuthorizeQuery);

    public static PresenceAuthorizeResponse? DeserializePresenceAuthorizeResponse(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.PresenceAuthorizeResponse);

    public static MessageHistoryPage? DeserializeMessageHistoryPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageHistoryPage);

    public static ConversationListPage? DeserializeConversationListPage(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationListPage);

    public static ConversationMarkReadResult? DeserializeConversationMarkReadResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationMarkReadResult);

    public static ConversationSetPrefsResult? DeserializeConversationSetPrefsResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.ConversationSetPrefsResult);

    public static GroupConversationResult? DeserializeGroupConversationResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.GroupConversationResult);

    public static MessageRecallResult? DeserializeMessageRecallResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageRecallResult);

    public static MessageEditResult? DeserializeMessageEditResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageEditResult);

    public static MessageReactionResult? DeserializeMessageReactionResult(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.MessageReactionResult);

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

    public static RealtimeMessageEditedPayload? DeserializeMessageEdited(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMessageEditedPayload);

    public static RealtimeReactionAddedPayload? DeserializeReactionAdded(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeReactionAddedPayload);

    public static RealtimeReactionRemovedPayload? DeserializeReactionRemoved(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeReactionRemovedPayload);

    public static RealtimeMemberJoinedPayload? DeserializeMemberJoined(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMemberJoinedPayload);

    public static RealtimeMemberLeftPayload? DeserializeMemberLeft(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMemberLeftPayload);

    public static RealtimeMemberRemovedPayload? DeserializeMemberRemoved(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeMemberRemovedPayload);

    public static RealtimeRoleChangedPayload? DeserializeRoleChanged(string json) =>
        JsonSerializer.Deserialize(json, RealtimeIntegrationJsonContext.Default.RealtimeRoleChangedPayload);

    public static string Serialize(RealtimeDomainNotificationPayload payload) =>
        JsonSerializer.Serialize(payload, RealtimeIntegrationJsonContext.Default.RealtimeDomainNotificationPayload);
}

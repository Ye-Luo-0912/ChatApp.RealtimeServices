using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Integration.Serialization;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(IncomingMessageCommand))]
[JsonSerializable(typeof(MessageReceiptCommand))]
[JsonSerializable(typeof(RealtimeEvent))]
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(RealtimeChatMessagePayload))]
[JsonSerializable(typeof(AttachmentRef))]
[JsonSerializable(typeof(List<AttachmentRef>))]
[JsonSerializable(typeof(RealtimeConversationChangedPayload))]
[JsonSerializable(typeof(RealtimeMessageReceiptPayload))]
[JsonSerializable(typeof(RealtimeDomainNotificationPayload))]
[JsonSerializable(typeof(MessageHistoryQuery))]
[JsonSerializable(typeof(MessageHistoryPage))]
[JsonSerializable(typeof(MessageHistoryCursor))]
[JsonSerializable(typeof(RealtimeHistoryMessage))]
[JsonSerializable(typeof(List<RealtimeHistoryMessage>))]
[JsonSerializable(typeof(ConversationListQuery))]
[JsonSerializable(typeof(ConversationListPage))]
[JsonSerializable(typeof(ConversationListItem))]
[JsonSerializable(typeof(ConversationListCursor))]
[JsonSerializable(typeof(List<ConversationListItem>))]
[JsonSerializable(typeof(ConversationMarkReadCommand))]
[JsonSerializable(typeof(ConversationMarkReadResult))]
[JsonSerializable(typeof(ConversationSetPrefsCommand))]
[JsonSerializable(typeof(ConversationSetPrefsResult))]
[JsonSerializable(typeof(MessageRecallCommand))]
[JsonSerializable(typeof(MessageRecallResult))]
[JsonSerializable(typeof(RealtimeMessageRecalledPayload))]
[JsonSerializable(typeof(SyncBootstrapQuery))]
[JsonSerializable(typeof(SyncBootstrapPage))]
[JsonSerializable(typeof(ConversationSyncWatermark))]
[JsonSerializable(typeof(List<ConversationSyncWatermark>))]
[JsonSerializable(typeof(ConversationHistoryCatchUp))]
[JsonSerializable(typeof(List<ConversationHistoryCatchUp>))]
[JsonSerializable(typeof(RealtimeUnreadCountChangedPayload))]
[JsonSerializable(typeof(AttachmentBlobsPurgePayload))]
internal sealed partial class RealtimeIntegrationJsonContext : JsonSerializerContext
{
}

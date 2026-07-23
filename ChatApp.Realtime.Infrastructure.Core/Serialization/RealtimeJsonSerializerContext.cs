using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Infrastructure.Core.Serialization;

[JsonSerializable(typeof(RealtimeEvent))]
[JsonSerializable(typeof(IncomingMessageCommand))]
[JsonSerializable(typeof(MessageProcessResult))]
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(RealtimeChatMessagePayload))]
[JsonSerializable(typeof(AttachmentRef))]
[JsonSerializable(typeof(List<AttachmentRef>))]
[JsonSerializable(typeof(RealtimeConversationChangedPayload))]
[JsonSerializable(typeof(RealtimeUnreadCountChangedPayload))]
[JsonSerializable(typeof(AttachmentBlobsPurgePayload))]
[JsonSerializable(typeof(ConversationListQuery))]
[JsonSerializable(typeof(ConversationListPage))]
[JsonSerializable(typeof(ConversationListCursor))]
[JsonSerializable(typeof(ConversationListItem))]
[JsonSerializable(typeof(List<ConversationListItem>))]
[JsonSerializable(typeof(ConversationMarkReadCommand))]
[JsonSerializable(typeof(ConversationMarkReadResult))]
[JsonSerializable(typeof(ConversationSetPrefsCommand))]
[JsonSerializable(typeof(ConversationSetPrefsResult))]
[JsonSerializable(typeof(MessageRecallCommand))]
[JsonSerializable(typeof(MessageRecallResult))]
[JsonSerializable(typeof(RealtimeMessageRecalledPayload))]
[JsonSerializable(typeof(MessageReceiptCommand))]
[JsonSerializable(typeof(RealtimeMessageReceiptPayload))]
[JsonSerializable(typeof(RealtimeDomainNotificationPayload))]
[JsonSerializable(typeof(MessageHistoryQuery))]
[JsonSerializable(typeof(MessageHistoryPage))]
[JsonSerializable(typeof(MessageHistoryCursor))]
[JsonSerializable(typeof(RealtimeHistoryMessage))]
[JsonSerializable(typeof(List<RealtimeHistoryMessage>))]
[JsonSerializable(typeof(SyncBootstrapQuery))]
[JsonSerializable(typeof(SyncBootstrapPage))]
[JsonSerializable(typeof(ConversationSyncWatermark))]
[JsonSerializable(typeof(List<ConversationSyncWatermark>))]
[JsonSerializable(typeof(ConversationHistoryCatchUp))]
[JsonSerializable(typeof(List<ConversationHistoryCatchUp>))]
public sealed partial class RealtimeJsonSerializerContext : JsonSerializerContext
{
}

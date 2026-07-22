using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Infrastructure.Core.Serialization;

[JsonSerializable(typeof(RealtimeEvent))]
[JsonSerializable(typeof(IncomingMessageCommand))]
[JsonSerializable(typeof(MessageProcessResult))]
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(RealtimeChatMessagePayload))]
[JsonSerializable(typeof(MessageReceiptCommand))]
[JsonSerializable(typeof(RealtimeMessageReceiptPayload))]
[JsonSerializable(typeof(RealtimeDomainNotificationPayload))]
[JsonSerializable(typeof(MessageHistoryQuery))]
[JsonSerializable(typeof(MessageHistoryPage))]
[JsonSerializable(typeof(MessageHistoryCursor))]
[JsonSerializable(typeof(RealtimeHistoryMessage))]
[JsonSerializable(typeof(List<RealtimeHistoryMessage>))]
public sealed partial class RealtimeJsonSerializerContext : JsonSerializerContext
{
}

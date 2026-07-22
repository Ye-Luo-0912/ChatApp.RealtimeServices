using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Integration.Serialization;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(IncomingMessageCommand))]
[JsonSerializable(typeof(MessageReceiptCommand))]
[JsonSerializable(typeof(RealtimeEvent))]
[JsonSerializable(typeof(DeadLetterMessage))]
[JsonSerializable(typeof(RealtimeChatMessagePayload))]
[JsonSerializable(typeof(RealtimeMessageReceiptPayload))]
[JsonSerializable(typeof(RealtimeDomainNotificationPayload))]
[JsonSerializable(typeof(MessageHistoryQuery))]
[JsonSerializable(typeof(MessageHistoryPage))]
[JsonSerializable(typeof(MessageHistoryCursor))]
[JsonSerializable(typeof(RealtimeHistoryMessage))]
[JsonSerializable(typeof(List<RealtimeHistoryMessage>))]
internal sealed partial class RealtimeIntegrationJsonContext : JsonSerializerContext
{
}

using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Infrastructure.Core.Serialization;

[JsonSerializable(typeof(RealtimeEvent))]
[JsonSerializable(typeof(IncomingMessageCommand))]
[JsonSerializable(typeof(MessageProcessResult))]
public sealed partial class RealtimeJsonSerializerContext : JsonSerializerContext
{
}

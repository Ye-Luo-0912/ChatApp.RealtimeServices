using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Integration.Outbox;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(RealtimeEvent))]
internal sealed partial class RealtimeOutboxJsonContext : JsonSerializerContext
{
}

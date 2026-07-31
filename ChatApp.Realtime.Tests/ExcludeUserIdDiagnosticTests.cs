using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Tests;

public sealed class ExcludeUserIdDiagnosticTests
{
    [Fact]
    public void Serialize_RealtimeEvent_WithExcludeUserId_PreservesValue()
    {
        var evt = new RealtimeEvent
        {
            EventId = "diag-1",
            Type = RealtimeEventType.ConversationRead,
            TargetUserId = 502,
            ActorUserId = 502,
            MessageId = "g-msg-1",
            PayloadJson = "{}",
            OccurredAtMs = 1_700_000_000_000,
            AudienceKind = AudienceKind.Conversation,
            ConversationId = "grp:diag",
            ExcludeUserId = 502
        };

        var json = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);
        Assert.Contains("\"ExcludeUserId\":502", json);

        var deserialized = JsonSerializer.Deserialize(json, RealtimeJsonSerializerContext.Default.RealtimeEvent);
        Assert.NotNull(deserialized);
        Assert.Equal(502L, deserialized!.ExcludeUserId);
    }

    [Fact]
    public void Serialize_RealtimeEvent_WithExcludeUserId_PreservesValue_NonNullable()
    {
        // Use non-nullable long directly
        long excludeUserId = 502L;
        var evt = new RealtimeEvent
        {
            EventId = "diag-2",
            Type = RealtimeEventType.ConversationRead,
            TargetUserId = 502,
            ActorUserId = 502,
            MessageId = "g-msg-2",
            PayloadJson = "{}",
            OccurredAtMs = 1_700_000_000_000,
            AudienceKind = AudienceKind.Conversation,
            ConversationId = "grp:diag",
            ExcludeUserId = excludeUserId
        };

        var json = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);
        Assert.Contains("\"ExcludeUserId\":502", json);
    }
}

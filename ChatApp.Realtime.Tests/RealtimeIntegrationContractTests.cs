using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Outbox;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeIntegrationContractTests
{
    [Fact]
    public void EventWireFormat_RoundTripsAcrossIntegrationBoundary()
    {
        var original = new RealtimeEvent
        {
            EventId = "event-1",
            Type = RealtimeEventType.FriendRequestListChanged,
            TargetUserId = 1002,
            ActorUserId = 1001,
            PayloadJson = RealtimeWireSerializer.Serialize(new RealtimeDomainNotificationPayload
            {
                Resource = "friend-request",
                Action = "Pending",
                ResourceId = "42",
                Message = "hello"
            }),
            OccurredAtMs = 123,
            TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            TraceState = "vendor=value"
        };

        var restored = RealtimeWireSerializer.DeserializeEvent(RealtimeWireSerializer.Serialize(original));

        Assert.NotNull(restored);
        Assert.Equal(original.EventId, restored.EventId);
        Assert.Equal(original.Type, restored.Type);
        Assert.Equal(original.TargetUserId, restored.TargetUserId);
        Assert.Equal(original.PayloadJson, restored.PayloadJson);
        Assert.Equal(original.TraceParent, restored.TraceParent);
        Assert.Equal(original.TraceState, restored.TraceState);
    }

    [Fact]
    public void OutboxItem_UsesWireEventAndImmediateAttemptTime()
    {
        var evt = new RealtimeEvent
        {
            EventId = "event-2",
            Type = RealtimeEventType.BlockedListChanged,
            TargetUserId = 1002
        };

        var item = RealtimeIntegrationOutboxItem.FromEvent(evt);
        var restored = RealtimeWireSerializer.DeserializeEvent(item.PayloadJson);

        Assert.Equal(item.CreatedAtMs, item.NextAttemptAtMs);
        Assert.Equal(evt.EventId, item.EventId);
        Assert.Equal(evt.TargetUserId, item.TargetUserId);
        Assert.Equal((short)evt.Type, item.EventType);
        Assert.Equal(evt.EventId, restored!.EventId);
    }
}

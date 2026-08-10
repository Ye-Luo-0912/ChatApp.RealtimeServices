using System.Text;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.RealtimeServices.Workers;

internal static class RelationshipProjectionPrePublisher
{
    public static async Task ApplyAsync(
        RealtimeEvent sourceEvent,
        IRelationshipProjectionStore? store,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(sourceEvent);
        if (!IsRelationshipEvent(sourceEvent.Type)
            || sourceEvent.PayloadJson is not { Length: > 0 } payloadJson)
        {
            return;
        }

        var notification = RealtimeWireSerializer.DeserializeDomainNotification(payloadJson)
            ?? throw new InvalidDataException("Relationship notification payload is null.");
        var delta = notification.Projection;
        if (delta is null)
            return;

        if (!string.Equals(sourceEvent.EventId, delta.EventId, StringComparison.Ordinal)
            || sourceEvent.TargetUserId != delta.OwnerUserId)
        {
            throw new InvalidDataException(
                "Relationship projection envelope does not match its versioned delta.");
        }

        var projectionStore = store
            ?? throw new InvalidOperationException("Relationship projection store is unavailable.");
        await projectionStore.ApplyAsync(delta, ct).ConfigureAwait(false);
    }

    public static async Task ApplyAsync(
        RealtimeOutboxRecord record,
        IRelationshipProjectionStore? store,
        CancellationToken ct)
    {
        if (!IsRelationshipEvent(record.EventType))
            return;

        var sourceEvent = record.Event;
        if (sourceEvent is null && record.PayloadUtf8 is { Length: > 0 } payloadUtf8)
        {
            sourceEvent = RealtimeWireSerializer.DeserializeEvent(
                Encoding.UTF8.GetString(payloadUtf8.Span));
        }

        if (sourceEvent is null)
            return;

        if (!string.Equals(record.EventId, sourceEvent.EventId, StringComparison.Ordinal)
            || record.TargetUserId != sourceEvent.TargetUserId
            || sourceEvent.Type != record.EventType)
        {
            throw new InvalidDataException(
                "Relationship Outbox record does not match its event envelope.");
        }

        await ApplyAsync(sourceEvent, store, ct).ConfigureAwait(false);
    }

    private static bool IsRelationshipEvent(RealtimeEventType eventType) =>
        eventType is RealtimeEventType.FriendRequestListChanged
            or RealtimeEventType.FriendListChanged
            or RealtimeEventType.BlockedListChanged;
}

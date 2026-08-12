using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.RealtimeServices.Workers;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class RelationshipProjectionPrePublisherTests
{
    [Fact]
    public async Task ServerOriginatedInboundEvent_AppliesBeforeJetStreamAckBoundary()
    {
        var delta = CreateDelta(1000, RelationshipProjectionListType.Friends, 1);
        var store = new RecordingStore();
        var evt = CreateRecord(delta, includeLegacyEvent: true).Event!;

        await RelationshipProjectionPrePublisher.ApplyAsync(evt, store, CancellationToken.None);

        Assert.NotNull(store.Applied);
        Assert.Equal(delta.EventId, store.Applied.EventId);
        Assert.Equal(delta.OwnerUserId, store.Applied.OwnerUserId);
    }

    [Fact]
    public async Task VersionedRelationshipEvent_AppliesBeforePublishBoundary()
    {
        var delta = CreateDelta(1001, RelationshipProjectionListType.Friends, 1);
        var store = new RecordingStore();
        var record = CreateRecord(delta, includeLegacyEvent: true);

        await RelationshipProjectionPrePublisher.ApplyAsync(record, store, CancellationToken.None);

        Assert.NotNull(store.Applied);
        Assert.Equal(delta.EventId, store.Applied.EventId);
        Assert.Equal(delta.OwnerUserId, store.Applied.OwnerUserId);
        Assert.Equal(delta.Version, store.Applied.Version);
    }

    [Fact]
    public async Task TypedUtf8RelationshipEvent_AppliesWithoutLegacyEventObject()
    {
        var delta = CreateDelta(1002, RelationshipProjectionListType.BlockedUsers, 1);
        var store = new RecordingStore();
        var record = CreateRecord(delta, includeLegacyEvent: false);

        await RelationshipProjectionPrePublisher.ApplyAsync(record, store, CancellationToken.None);

        Assert.NotNull(store.Applied);
        Assert.Equal(delta.EventId, store.Applied.EventId);
    }

    [Fact]
    public async Task MismatchedEnvelope_IsRejectedWithoutApplyingProjection()
    {
        var delta = CreateDelta(1003, RelationshipProjectionListType.FriendRequests, 1);
        var store = new RecordingStore();
        var record = CreateRecord(delta, includeLegacyEvent: true) with { TargetUserId = 9999 };

        await Assert.ThrowsAsync<InvalidDataException>(
            () => RelationshipProjectionPrePublisher.ApplyAsync(
                record, store, CancellationToken.None));

        Assert.Null(store.Applied);
    }

    [Fact]
    public async Task LegacyInvalidationWithoutProjection_RemainsCompatible()
    {
        var evt = new RealtimeEvent
        {
            EventId = "legacy-event",
            Type = RealtimeEventType.FriendListChanged,
            TargetUserId = 1004,
            PayloadJson = RealtimeWireSerializer.Serialize(new RealtimeDomainNotificationPayload
            {
                Resource = "friendship",
                Action = "Upsert",
                ResourceId = "1005"
            })
        };
        var store = new RecordingStore();
        var record = new RealtimeOutboxRecord(
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            null,
            AudienceKind.User,
            null,
            null,
            null,
            null,
            evt,
            1,
            "test",
            "claim");

        await RelationshipProjectionPrePublisher.ApplyAsync(record, store, CancellationToken.None);

        Assert.Null(store.Applied);
    }

    private static RealtimeOutboxRecord CreateRecord(
        RelationshipProjectionDelta delta,
        bool includeLegacyEvent)
    {
        var payload = new RealtimeDomainNotificationPayload
        {
            Resource = "relationship",
            Action = delta.State ?? "Upsert",
            ResourceId = delta.ResourceId,
            Projection = delta
        };
        var eventType = delta.ListType switch
        {
            RelationshipProjectionListType.FriendRequests => RealtimeEventType.FriendRequestListChanged,
            RelationshipProjectionListType.Friends => RealtimeEventType.FriendListChanged,
            RelationshipProjectionListType.BlockedUsers => RealtimeEventType.BlockedListChanged,
            _ => throw new ArgumentOutOfRangeException()
        };
        var evt = new RealtimeEvent
        {
            EventId = delta.EventId,
            Type = eventType,
            TargetUserId = delta.OwnerUserId,
            ActorUserId = delta.ActorUserId,
            PayloadJson = RealtimeWireSerializer.Serialize(payload),
            OccurredAtMs = delta.OccurredAtMs
        };

        return new RealtimeOutboxRecord(
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            null,
            AudienceKind.User,
            null,
            null,
            null,
            null,
            includeLegacyEvent ? evt : null,
            1,
            "test",
            "claim",
            includeLegacyEvent ? null : RealtimeEventWireSerializer.SerializeToUtf8Bytes(evt));
    }

    private static RelationshipProjectionDelta CreateDelta(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long version)
    {
        var eventId = RelationshipEventIdFactory.CreateRelationshipProjectionEventId(
            ownerUserId, listType, version);
        return new RelationshipProjectionDelta
        {
            EventId = eventId,
            OwnerUserId = ownerUserId,
            ListType = listType,
            Version = version,
            Operation = RelationshipProjectionOperation.Upsert,
            ResourceId = "resource-1",
            SubjectUserId = ownerUserId + 1,
            ActorUserId = ownerUserId,
            State = "Upsert",
            OccurredAtMs = 1_800_000_000_000
        };
    }

    private sealed class RecordingStore : IRelationshipProjectionStore
    {
        public RelationshipProjectionDelta? Applied { get; private set; }

        public Task<RelationshipProjectionApplyResult> ApplyAsync(
            RelationshipProjectionDelta delta,
            CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            Applied = delta;
            return Task.FromResult(RelationshipProjectionApplyResult.Applied);
        }

        public Task<RelationshipProjectionSnapshotApplyResult> ApplySnapshotAsync(
            RelationshipProjectionStreamSnapshot snapshot,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<RelationshipProjectionHistoryEntry>> QueryHistoryAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            long fromVersionExclusive,
            int limit,
            CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}

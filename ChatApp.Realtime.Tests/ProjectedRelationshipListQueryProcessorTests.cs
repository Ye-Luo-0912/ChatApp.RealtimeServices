using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Relationships;

namespace ChatApp.Realtime.Tests;

public sealed class ProjectedRelationshipListQueryProcessorTests
{
    [Theory]
    [InlineData(RelationshipListType.Friends, RelationshipProjectionListType.Friends)]
    [InlineData(RelationshipListType.FriendRequests, RelationshipProjectionListType.FriendRequests)]
    [InlineData(RelationshipListType.BlockedUsers, RelationshipProjectionListType.BlockedUsers)]
    public async Task MapsPublicListTypeExplicitly(
        RelationshipListType input,
        RelationshipProjectionListType expected)
    {
        var store = new RecordingQueryStore(
            Ready(7, [Item("resource-1")], false, null));
        var processor = new ProjectedRelationshipListQueryProcessor(store);

        var result = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "mapping",
            ActorUserId = 42,
            ListType = input
        });

        Assert.True(result.Succeeded);
        Assert.Equal(expected, Assert.Single(store.Calls).ListType);
    }

    [Fact]
    public async Task CursorCarriesVersionAndResourceIdAcrossPages()
    {
        var store = new RecordingQueryStore(
            Ready(12, [Item("resource-a")], true, "resource-a"),
            Ready(12, [Item("resource-b")], false, null));
        var processor = new ProjectedRelationshipListQueryProcessor(store);

        var first = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "page-1",
            ActorUserId = 42,
            ListType = RelationshipListType.Friends,
            PageSize = 1
        });
        Assert.True(first.Succeeded);
        Assert.True(first.HasMore);
        Assert.False(string.IsNullOrWhiteSpace(first.NextCursor));

        var second = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "page-2",
            ActorUserId = 42,
            ListType = RelationshipListType.Friends,
            PageSize = 1,
            Cursor = first.NextCursor
        });

        Assert.True(second.Succeeded);
        Assert.False(second.HasMore);
        Assert.Equal(2, store.Calls.Count);
        Assert.Equal(12, store.Calls[1].ExpectedVersion);
        Assert.Equal("resource-a", store.Calls[1].AfterResourceId);
    }

    [Fact]
    public async Task InvalidCursorFailsBeforeStoreAccess()
    {
        var store = new RecordingQueryStore(Ready(1, [], false, null));
        var processor = new ProjectedRelationshipListQueryProcessor(store);

        var result = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "bad-cursor",
            ActorUserId = 42,
            ListType = RelationshipListType.Friends,
            Cursor = "not-base64"
        });

        Assert.False(result.Succeeded);
        Assert.Equal("invalid_cursor", result.ErrorCode);
        Assert.Empty(store.Calls);
    }

    [Theory]
    [InlineData(RelationshipProjectionReadStatus.Unavailable, "relationship_read_projection_unavailable")]
    [InlineData(RelationshipProjectionReadStatus.VersionChanged, "relationship_projection_changed")]
    public async Task ProjectionReadFailureIsExplicit(
        RelationshipProjectionReadStatus status,
        string expectedError)
    {
        var store = new RecordingQueryStore(new RelationshipProjectionReadPage(
            status,
            9,
            [],
            false,
            null));
        var processor = new ProjectedRelationshipListQueryProcessor(store);

        var result = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "projection-state",
            ActorUserId = 42,
            ListType = RelationshipListType.Friends
        });

        Assert.False(result.Succeeded);
        Assert.Equal(expectedError, result.ErrorCode);
    }

    private static RelationshipProjectionReadPage Ready(
        long version,
        IReadOnlyList<RelationshipListItem> items,
        bool hasMore,
        string? nextResourceId) =>
        new(RelationshipProjectionReadStatus.Ready, version, items, hasMore, nextResourceId);

    private static RelationshipListItem Item(string resourceId) => new()
    {
        UserId = 43,
        ResourceId = resourceId,
        Status = "Accepted",
        CreatedAtMs = 1_800_000_000_000
    };

    private sealed class RecordingQueryStore(
        params RelationshipProjectionReadPage[] responses) : IRelationshipProjectionQueryStore
    {
        private int _nextResponse;

        public List<Call> Calls { get; } = [];

        public Task<RelationshipProjectionReadPage> ReadAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            int pageSize,
            long? expectedVersion,
            string? afterResourceId,
            CancellationToken ct = default)
        {
            Calls.Add(new Call(
                ownerUserId,
                listType,
                pageSize,
                expectedVersion,
                afterResourceId));
            return Task.FromResult(responses[_nextResponse++]);
        }
    }

    private sealed record Call(
        long OwnerUserId,
        RelationshipProjectionListType ListType,
        int PageSize,
        long? ExpectedVersion,
        string? AfterResourceId);
}

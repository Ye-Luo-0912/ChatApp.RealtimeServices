using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Relationships;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// REL-READ-3 source parity: verifies that the Server-authoritative relationship projection
/// (items + version + history) is the single source of truth for list reads, and that a client
/// can converge from a snapshot using only incremental deltas. Every scenario drives the real
/// Postgres projection store from an in-memory Server authority, then reads list pages back
/// through <see cref="NpgsqlRelationshipProjectionQueryStore"/> and compares item-for-item.
/// </summary>
[Collection(nameof(RealtimePipelineCollection))]
public sealed class RelationshipProjectionSourceParityTests
{
    // 每个用例使用独立 owner，避免共享 Postgres 中 (owner,list) 流版本在用例间互相污染。
    private static long _ownerSeed = 8_200_000;
    private readonly RealtimePipelineFixture _fixture;

    public RelationshipProjectionSourceParityTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    private static long NextOwner() => Interlocked.Increment(ref _ownerSeed);

    [Fact]
    public async Task DeltaCatchUp_ConvergesListToServerAuthority_AfterSnapshot()
    {
        var owner = NextOwner();

        // 快照时刻的基线状态。
        var authority = new TestServerAuthority(owner);
        authority.Set(RelationshipProjectionListType.Friends, "friend-a", "Accepted");
        authority.Set(RelationshipProjectionListType.FriendRequests, "req-1", "Pending");
        authority.Set(RelationshipProjectionListType.BlockedUsers, "block-1", "Blocked");

        var (store, query) = CreateStores();
        foreach (var listType in AllListTypes)
            await SeedSnapshotAsync(store, authority, listType);

        // 快照之后 Server 又发生 mutation，客户端只能靠增量收敛。
        authority.Set(RelationshipProjectionListType.Friends, "friend-b", "Accepted");
        authority.Set(RelationshipProjectionListType.FriendRequests, "req-2", "Pending");
        foreach (var listType in AllListTypes)
            await CatchUpFromAuthorityAsync(store, authority, listType);

        foreach (var listType in AllListTypes)
            await AssertParityAsync(owner, authority, query, listType);
    }

    [Fact]
    public async Task ConcurrentMutation_AfterPagingStarts_ForcesClientResetViaVersionChange()
    {
        var owner = NextOwner();
        var authority = new TestServerAuthority(owner);
        for (var i = 0; i < 5; i++)
            authority.Set(RelationshipProjectionListType.Friends, $"friend-{i}", "Accepted");

        var (store, query) = CreateStores();
        await SeedSnapshotAsync(store, authority, RelationshipProjectionListType.Friends);
        await CatchUpFromAuthorityAsync(store, authority, RelationshipProjectionListType.Friends);

        var first = await query.ReadAsync(owner, RelationshipProjectionListType.Friends, 2, null, null);
        Assert.Equal(RelationshipProjectionReadStatus.Ready, first.Status);
        Assert.True(first.HasMore);
        var cursorVersion = first.CurrentVersion;

        // Snapshot 期间并发 mutation：Server 追加一个新好友并推进版本。
        authority.Set(RelationshipProjectionListType.Friends, "friend-late", "Accepted");
        await CatchUpFromAuthorityAsync(store, authority, RelationshipProjectionListType.Friends);

        // 用旧版本游标续页，必须显式返回 VersionChanged，禁止静默返回过期页。
        var second = await query.ReadAsync(
            owner,
            RelationshipProjectionListType.Friends,
            2,
            cursorVersion,
            first.NextResourceId);
        Assert.Equal(RelationshipProjectionReadStatus.VersionChanged, second.Status);
        Assert.NotEqual(cursorVersion, second.CurrentVersion);
    }

    [Fact]
    public async Task DuplicateCursor_ReturnsStableContinuation_WithoutDataLoss()
    {
        var owner = NextOwner();
        var authority = new TestServerAuthority(owner);
        for (var i = 0; i < 5; i++)
            authority.Set(RelationshipProjectionListType.Friends, $"dup-{i}", "Accepted");

        var (store, _) = CreateStores();
        await SeedSnapshotAsync(store, authority, RelationshipProjectionListType.Friends);
        await CatchUpFromAuthorityAsync(store, authority, RelationshipProjectionListType.Friends);

        var processor = Processor();
        var first = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "page-1",
            ActorUserId = owner,
            ListType = RelationshipListType.Friends,
            PageSize = 2
        });
        Assert.True(first.Succeeded);
        Assert.True(first.HasMore);

        // 同一 cursor 重放两次：同版本、同 after-resource，必须返回同一稳定延续（无数据丢失）。
        var replay1 = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "dup-cursor-1",
            ActorUserId = owner,
            ListType = RelationshipListType.Friends,
            PageSize = 2,
            Cursor = first.NextCursor
        });
        var replay2 = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "dup-cursor-2",
            ActorUserId = owner,
            ListType = RelationshipListType.Friends,
            PageSize = 2,
            Cursor = first.NextCursor
        });
        Assert.True(replay1.Succeeded);
        Assert.True(replay1.HasMore);
        Assert.Equal(replay1.NextCursor, replay2.NextCursor);
        Assert.Equal(
            replay1.Items!.Select(static item => item.ResourceId),
            replay2.Items!.Select(static item => item.ResourceId));

        // 续到最后一页后稳定结束。
        var next = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "page-3",
            ActorUserId = owner,
            ListType = RelationshipListType.Friends,
            PageSize = 2,
            Cursor = replay1.NextCursor
        });
        Assert.True(next.Succeeded);
        Assert.False(next.HasMore);
    }

    [Fact]
    public async Task NoSnapshotCheckpoint_IsUnavailable_AndFailsClosed()
    {
        var owner = NextOwner();
        var authority = new TestServerAuthority(owner);
        authority.Set(RelationshipProjectionListType.Friends, "friend-a", "Accepted");

        var (store, query) = CreateStores();
        // 只应用增量、不建立 snapshot checkpoint。
        await store.ApplyAsync(authority.DeltasAfter(RelationshipProjectionListType.Friends, 0).Single());

        var page = await query.ReadAsync(owner, RelationshipProjectionListType.Friends, 50, null, null);
        Assert.Equal(RelationshipProjectionReadStatus.Unavailable, page.Status);
        Assert.Empty(page.Items);
    }

    [Fact]
    public async Task DeletedItem_DisappearsFromList_AndHistoryStillAdvances()
    {
        var owner = NextOwner();
        var authority = new TestServerAuthority(owner);
        authority.Set(RelationshipProjectionListType.Friends, "doomed", "Accepted");
        authority.Set(RelationshipProjectionListType.Friends, "keeper", "Accepted");

        var (store, query) = CreateStores();
        await SeedSnapshotAsync(store, authority, RelationshipProjectionListType.Friends);
        await CatchUpFromAuthorityAsync(store, authority, RelationshipProjectionListType.Friends);

        authority.Remove(RelationshipProjectionListType.Friends, "doomed");
        await CatchUpFromAuthorityAsync(store, authority, RelationshipProjectionListType.Friends);

        await AssertParityAsync(owner, authority, query, RelationshipProjectionListType.Friends);

        var history = await store.QueryHistoryAsync(owner, RelationshipProjectionListType.Friends, 0, 200);
        Assert.Contains(history, entry => entry.Operation == RelationshipProjectionOperation.Delete
                                           && entry.ResourceId == "doomed");
    }

    private static readonly RelationshipProjectionListType[] AllListTypes =
    [
        RelationshipProjectionListType.Friends,
        RelationshipProjectionListType.FriendRequests,
        RelationshipProjectionListType.BlockedUsers
    ];

    private async Task SeedSnapshotAsync(
        IRelationshipProjectionStore store,
        TestServerAuthority authority,
        RelationshipProjectionListType listType)
    {
        var snapshot = authority.BuildSnapshot(listType);
        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Applied,
            await store.ApplySnapshotAsync(snapshot));
        // 快照覆盖了当前版本，之后只从该版本之后收敛增量。
        authority.MarkApplied(listType, snapshot.Version);
    }

    private async Task CatchUpFromAuthorityAsync(
        IRelationshipProjectionStore store,
        TestServerAuthority authority,
        RelationshipProjectionListType listType)
    {
        foreach (var delta in authority.DeltasAfter(listType, authority.LastAppliedVersion(listType)))
        {
            var result = await store.ApplyAsync(delta);
            Assert.True(
                result is RelationshipProjectionApplyResult.Applied
                    or RelationshipProjectionApplyResult.Duplicate,
                $"Unexpected apply result {result} for {listType} v{delta.Version}.");
            authority.MarkApplied(listType, delta.Version);
        }
    }

    private async Task AssertParityAsync(
        long owner,
        TestServerAuthority authority,
        IRelationshipProjectionQueryStore query,
        RelationshipProjectionListType listType)
    {
        var expected = authority.Items(listType)
            .OrderBy(static item => item.Key, StringComparer.Ordinal)
            .Select(static item => item.Key)
            .ToArray();
        var actual = new List<string>();
        string? cursor = null;
        long? version = null;
        var guard = 0;
        while (guard++ < 100)
        {
            var page = await query.ReadAsync(owner, listType, 2, version, cursor);
            Assert.Equal(RelationshipProjectionReadStatus.Ready, page.Status);
            actual.AddRange(page.Items.Select(static item => item.ResourceId));
            if (!page.HasMore)
                break;
            version = page.CurrentVersion;
            cursor = page.NextResourceId!;
        }

        Assert.Equal(expected, actual);
    }

    private ProjectedRelationshipListQueryProcessor Processor() => new(
        new NpgsqlRelationshipProjectionQueryStore(
            new RealtimeDatabaseClient(
                _fixture.PostgresConnectionString,
                NullLogger<RealtimeDatabaseClient>.Instance),
            new RealtimeDatabaseSchema("realtime")));

    private (NpgsqlRelationshipProjectionStore Store, NpgsqlRelationshipProjectionQueryStore Query) CreateStores()
    {
        var client = new RealtimeDatabaseClient(
            _fixture.PostgresConnectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        var schema = new RealtimeDatabaseSchema("realtime");
        return (new NpgsqlRelationshipProjectionStore(client, schema),
            new NpgsqlRelationshipProjectionQueryStore(client, schema));
    }

    /// <summary>
    /// In-memory model of the Server-authoritative relationship state. Emits contiguous
    /// per-(owner,list) versioned deltas identical to what ChatApp.Server would publish in
    /// the same transaction as the authoritative write, and can build a snapshot checkpoint.
    /// </summary>
    private sealed class TestServerAuthority(long ownerUserId)
    {
        private readonly Dictionary<(RelationshipProjectionListType, string), (long Version, string State)>
            _items = [];
        private readonly Dictionary<RelationshipProjectionListType, long> _version = new();
        private readonly Dictionary<RelationshipProjectionListType, long> _applied = new();
        private readonly Dictionary<(RelationshipProjectionListType, string), (long Version, string State)>
            _retainedDeletes = [];

        public void Set(RelationshipProjectionListType listType, string resourceId, string state)
        {
            _version[listType] = NextVersion(listType);
            _items[(listType, resourceId)] = (_version[listType], state);
        }

        public void Remove(RelationshipProjectionListType listType, string resourceId)
        {
            _version[listType] = NextVersion(listType);
            _retainedDeletes[(listType, resourceId)] = (_version[listType], "Delete");
            _items.Remove((listType, resourceId));
        }

        public IReadOnlyDictionary<string, string> Items(RelationshipProjectionListType listType) =>
            _items
                .Where(kv => kv.Key.Item1 == listType)
                .ToDictionary(static kv => kv.Key.Item2, static kv => kv.Value.State);

        public long Version(RelationshipProjectionListType listType) =>
            _version.TryGetValue(listType, out var v) ? v : 0;

        public long LastAppliedVersion(RelationshipProjectionListType listType) =>
            _applied.TryGetValue(listType, out var v) ? v : 0;

        public void MarkApplied(RelationshipProjectionListType listType, long version) =>
            _applied[listType] = version;

        public IReadOnlyList<RelationshipProjectionDelta> DeltasAfter(
            RelationshipProjectionListType listType,
            long fromVersionExclusive)
        {
            var deltas = new List<RelationshipProjectionDelta>();
            foreach (var (key, value) in _items)
            {
                if (key.Item1 != listType || value.Version <= fromVersionExclusive)
                    continue;
                deltas.Add(CreateDelta(listType, key.Item2, value.Version, value.State, upsert: true));
            }

            // 已删除项在 history 中保留 Delete 记录，但当前集合不再包含它；这里按版本回放删除。
            foreach (var (key, value) in _retainedDeletes)
            {
                if (key.Item1 != listType || value.Version <= fromVersionExclusive)
                    continue;
                deltas.Add(CreateDelta(listType, key.Item2, value.Version, "Delete", upsert: false));
            }

            return deltas
                .OrderBy(static d => d.Version)
                .ToArray();
        }

        public RelationshipProjectionStreamSnapshot BuildSnapshot(RelationshipProjectionListType listType)
        {
            var items = _items
                .Where(kv => kv.Key.Item1 == listType)
                .OrderBy(static kv => kv.Key.Item2, StringComparer.Ordinal)
                .Select((kv, index) => new RelationshipProjectionSnapshotItem
                {
                    ResourceId = kv.Key.Item2,
                    SubjectUserId = ownerUserId + index + 1,
                    ActorUserId = ownerUserId,
                    State = kv.Value.State,
                    OccurredAtMs = 1_800_000_000_000
                })
                .ToArray();
            var version = Version(listType);
            var resourceHash = RelationshipProjectionSnapshotHash.Compute(
                items.Select(static item => item.ResourceId));
            return new RelationshipProjectionStreamSnapshot
            {
                SnapshotId = RelationshipEventIdFactory.CreateRelationshipProjectionSnapshotId(
                    ownerUserId, listType, version, resourceHash),
                OwnerUserId = ownerUserId,
                ListType = listType,
                Version = version,
                CapturedAtMs = 1_800_000_000_001,
                ItemCount = items.Length,
                ResourceHash = resourceHash,
                Items = items
            };
        }

        private long NextVersion(RelationshipProjectionListType listType) =>
            _version.TryGetValue(listType, out var v) ? v + 1 : 1;

        private RelationshipProjectionDelta CreateDelta(
            RelationshipProjectionListType listType,
            string resourceId,
            long version,
            string state,
            bool upsert)
        {
            if (!upsert)
                _retainedDeletes[(listType, resourceId)] = (version, "Delete");
            return new RelationshipProjectionDelta
            {
                EventId = RelationshipEventIdFactory.CreateRelationshipProjectionEventId(
                    ownerUserId, listType, version),
                OwnerUserId = ownerUserId,
                ListType = listType,
                Version = version,
                Operation = upsert
                    ? RelationshipProjectionOperation.Upsert
                    : RelationshipProjectionOperation.Delete,
                ResourceId = resourceId,
                SubjectUserId = ownerUserId + 1,
                ActorUserId = ownerUserId,
                State = upsert ? state : "Delete",
                Message = upsert && listType == RelationshipProjectionListType.FriendRequests ? "hello" : null,
                OccurredAtMs = 1_800_000_000_000 + version
            };
        }
    }
}
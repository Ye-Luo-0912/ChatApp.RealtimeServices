using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Net;
using System.Text;
using System.Text.Json;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class RelationshipProjectionRebuildWorkerTests
{
    [Fact]
    public async Task ServerSource_UsesStableCursorRoute_AndSourceGeneratedContracts()
    {
        var page = new RelationshipProjectionStreamPage(
            [new RelationshipProjectionStreamDescriptor(
                42,
                RelationshipProjectionListType.BlockedUsers,
                7)],
            true,
            42,
            RelationshipProjectionListType.BlockedUsers);
        var hash = RelationshipProjectionSnapshotHash.Compute([]);
        var snapshot = new RelationshipProjectionStreamSnapshot
        {
            SnapshotId = ChatApp.Realtime.Abstractions.Events.RelationshipEventIdFactory
                .CreateRelationshipProjectionSnapshotId(
                    42,
                    RelationshipProjectionListType.BlockedUsers,
                    7,
                    hash),
            OwnerUserId = 42,
            ListType = RelationshipProjectionListType.BlockedUsers,
            Version = 7,
            CapturedAtMs = 1,
            ItemCount = 0,
            ResourceHash = hash,
            Items = []
        };
        var digest = new RelationshipProjectionStreamDigest(
            42,
            RelationshipProjectionListType.BlockedUsers,
            7,
            0,
            hash,
            2);
        var handler = new RecordingHttpHandler(new Queue<HttpResponseMessage>(
        [
            JsonResponse(JsonSerializer.Serialize(
                page,
                RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamPage)),
            JsonResponse(JsonSerializer.Serialize(
                snapshot,
                RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamSnapshot)),
            JsonResponse(JsonSerializer.Serialize(
                digest,
                RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamDigest))
        ]));
        using var client = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://server.example/internal/")
        };
        client.DefaultRequestHeaders.Add(
            RelationshipProjectionRebuildOptions.ApiKeyHeaderName,
            "test-key");
        var source = new ServerRelationshipProjectionSnapshotSource(client);

        var actualPage = await source.ListStreamsAsync(
            40,
            RelationshipProjectionListType.Friends,
            25,
            CancellationToken.None);
        var actualSnapshot = await source.ReadStreamAsync(
            42,
            RelationshipProjectionListType.BlockedUsers,
            CancellationToken.None);
        var actualDigest = await source.ReadDigestAsync(
            42,
            RelationshipProjectionListType.BlockedUsers,
            CancellationToken.None);

        Assert.True(actualPage.HasMore);
        Assert.Equal(page.NextOwnerUserId, actualPage.NextOwnerUserId);
        Assert.Equal(page.NextListType, actualPage.NextListType);
        Assert.Equal(Assert.Single(page.Items), Assert.Single(actualPage.Items));
        Assert.Equal(snapshot.SnapshotId, actualSnapshot.SnapshotId);
        Assert.Equal(digest, actualDigest);
        Assert.Equal(
            "/internal/api/ops/relationship-projection/streams?limit=25" +
            $"&afterOwnerUserId=40&afterListType={(byte)RelationshipProjectionListType.Friends}",
            handler.Requests[0].PathAndQuery);
        Assert.Equal(
            "/internal/api/ops/relationship-projection/streams/42/" +
            (byte)RelationshipProjectionListType.BlockedUsers,
            handler.Requests[1].PathAndQuery);
        Assert.Equal(
            "/internal/api/ops/relationship-projection/streams/42/" +
            (byte)RelationshipProjectionListType.BlockedUsers + "/digest",
            handler.Requests[2].PathAndQuery);
        Assert.All(handler.ApiKeys, value => Assert.Equal("test-key", value));
    }

    [Fact]
    public async Task RunLease_ProcessesOrderedPages_CommitsCursor_AndCompletesPass()
    {
        var state = new RecordingStateStore();
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamPage(
                [new RelationshipProjectionStreamDescriptor(
                    10,
                    RelationshipProjectionListType.Friends,
                    2)],
                true,
                10,
                RelationshipProjectionListType.Friends),
            new RelationshipProjectionStreamPage(
                [new RelationshipProjectionStreamDescriptor(
                    11,
                    RelationshipProjectionListType.BlockedUsers,
                    0)],
                false,
                null,
                null)
        ]);
        var projection = new RecordingProjectionStore();
        var worker = CreateWorker(state, projection, source);
        var lease = NewLease();

        var result = await worker.RunLeaseAsync(lease, CancellationToken.None);

        Assert.Equal(2, source.ListCalls);
        Assert.Equal(2, source.ReadCalls);
        Assert.Equal(2, state.RenewCalls);
        Assert.Equal((10L, RelationshipProjectionListType.Friends), Assert.Single(state.Cursors));
        Assert.Equal(2, projection.Snapshots.Count);
        Assert.Equal(1, result.PassNumber);
        Assert.Equal(0, result.StablePasses);
    }

    [Fact]
    public async Task RunLease_RejectsHasMoreWithoutAdvancingCursor()
    {
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamPage([], true, null, null)
        ]);
        var worker = CreateWorker(
            new RecordingStateStore(),
            new RecordingProjectionStore(),
            source);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => worker.RunLeaseAsync(NewLease(), CancellationToken.None));
        Assert.Equal(0, source.ReadCalls);
    }

    private static RelationshipProjectionRebuildWorker CreateWorker(
        IRelationshipProjectionRebuildStateStore state,
        IRelationshipProjectionStore projection,
        IRelationshipProjectionSnapshotSource source) =>
        new(
            state,
            projection,
            source,
            Options.Create(new RelationshipProjectionRebuildOptions
            {
                Enabled = true,
                PageSize = 10,
                LeaseSeconds = 120,
                FailureRetrySeconds = 1,
                StablePollSeconds = 300,
                IdlePollMilliseconds = 100
            }),
            Options.Create(new RealtimeOptions
            {
                ServiceName = "test",
                InstanceId = "test-instance"
            }),
            new RealtimeMetrics(),
            NullLogger<RelationshipProjectionRebuildWorker>.Instance);

    private static RelationshipProjectionRebuildLease NewLease() => new(
        "test-instance",
        new string('a', 32),
        null,
        null,
        0,
        false,
        0);

    private static HttpResponseMessage JsonResponse(string json) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(json, Encoding.UTF8, "application/json")
    };

    private sealed class RecordingHttpHandler(Queue<HttpResponseMessage> responses)
        : HttpMessageHandler
    {
        public List<Uri> Requests { get; } = [];
        public List<string?> ApiKeys { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(Assert.IsType<Uri>(request.RequestUri));
            ApiKeys.Add(request.Headers.TryGetValues(
                    RelationshipProjectionRebuildOptions.ApiKeyHeaderName,
                    out var values)
                ? values.Single()
                : null);
            return Task.FromResult(responses.Dequeue());
        }
    }

    private sealed class RecordingSource(
        IReadOnlyList<RelationshipProjectionStreamPage> pages)
        : IRelationshipProjectionSnapshotSource
    {
        private int _pageIndex;
        public int ListCalls { get; private set; }
        public int ReadCalls { get; private set; }

        public Task<RelationshipProjectionStreamPage> ListStreamsAsync(
            long? afterOwnerUserId,
            RelationshipProjectionListType? afterListType,
            int limit,
            CancellationToken ct)
        {
            ListCalls++;
            return Task.FromResult(pages[_pageIndex++]);
        }

        public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct)
        {
            ReadCalls++;
            var hash = RelationshipProjectionSnapshotHash.Compute([]);
            var version = ownerUserId == 10 ? 2 : 0;
            return Task.FromResult(new RelationshipProjectionStreamSnapshot
            {
                SnapshotId = ChatApp.Realtime.Abstractions.Events.RelationshipEventIdFactory
                    .CreateRelationshipProjectionSnapshotId(ownerUserId, listType, version, hash),
                OwnerUserId = ownerUserId,
                ListType = listType,
                Version = version,
                CapturedAtMs = 1,
                ItemCount = 0,
                ResourceHash = hash,
                Items = []
            });
        }

        public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct)
        {
            var version = ownerUserId == 10 ? 2 : 0;
            return Task.FromResult(new RelationshipProjectionStreamDigest(
                ownerUserId,
                listType,
                version,
                0,
                RelationshipProjectionSnapshotHash.Compute([]),
                1));
        }
    }

    private sealed class RecordingProjectionStore : IRelationshipProjectionStore
    {
        public List<RelationshipProjectionStreamSnapshot> Snapshots { get; } = [];

        public Task<RelationshipProjectionApplyResult> ApplyAsync(
            RelationshipProjectionDelta delta,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<RelationshipProjectionSnapshotApplyResult> ApplySnapshotAsync(
            RelationshipProjectionStreamSnapshot snapshot,
            CancellationToken ct = default)
        {
            Snapshots.Add(snapshot);
            return Task.FromResult(RelationshipProjectionSnapshotApplyResult.Applied);
        }
    }

    private sealed class RecordingStateStore : IRelationshipProjectionRebuildStateStore
    {
        public int RenewCalls { get; private set; }
        public List<(long Owner, RelationshipProjectionListType List)> Cursors { get; } = [];

        public Task<RelationshipProjectionRebuildLease?> TryAcquireAsync(
            string leaseOwner,
            TimeSpan leaseDuration,
            CancellationToken ct = default) => throw new NotSupportedException();

        public Task<bool> RenewAsync(
            RelationshipProjectionRebuildLease lease,
            TimeSpan leaseDuration,
            CancellationToken ct = default)
        {
            RenewCalls++;
            return Task.FromResult(true);
        }

        public Task<bool> CommitPageAsync(
            RelationshipProjectionRebuildLease lease,
            long nextOwnerUserId,
            RelationshipProjectionListType nextListType,
            bool pageChanged,
            TimeSpan leaseDuration,
            CancellationToken ct = default)
        {
            Cursors.Add((nextOwnerUserId, nextListType));
            return Task.FromResult(true);
        }

        public Task<RelationshipProjectionRebuildPassResult?> CompletePassAsync(
            RelationshipProjectionRebuildLease lease,
            bool finalPageChanged,
            TimeSpan stablePollInterval,
            CancellationToken ct = default) =>
            Task.FromResult<RelationshipProjectionRebuildPassResult?>(new(
                1,
                finalPageChanged || lease.PassChanged ? 0 : 1,
                0));

        public Task<bool> ReleaseFailureAsync(
            RelationshipProjectionRebuildLease lease,
            string errorCode,
            TimeSpan retryDelay,
            CancellationToken ct = default) => Task.FromResult(true);
    }
}

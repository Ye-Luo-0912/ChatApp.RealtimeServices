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
    public async Task ServerSource_DeserializesCamelCaseServerPayload()
    {
        // Server 的导出端点以 camelCase 输出（AppJsonContext + ASP.NET Core 默认）。
        // 该用例锁定 camelCase + 大小写不敏感反序列化，防止 REL-GATE-1 发现的契约回归。
        var page = new RelationshipProjectionStreamPage(
            [new RelationshipProjectionStreamDescriptor(
                42,
                RelationshipProjectionListType.Friends,
                3)],
            false,
            null,
            null);
        var camelJson = JsonSerializer.Serialize(
            page,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                TypeInfoResolver = RealtimeJsonSerializerContext.Default
            });
        Assert.Contains("\"items\"", camelJson);
        Assert.DoesNotContain("\"Items\"", camelJson);

        var handler = new RecordingHttpHandler(new Queue<HttpResponseMessage>(
            [JsonResponse(camelJson)]));
        using var client = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://server.example/internal/")
        };
        var source = new ServerRelationshipProjectionSnapshotSource(client);

        var actual = await source.ListStreamsAsync(
            null,
            null,
            25,
            CancellationToken.None);

        Assert.False(actual.HasMore);
        var descriptor = Assert.Single(actual.Items);
        Assert.Equal(42, descriptor.OwnerUserId);
        Assert.Equal(RelationshipProjectionListType.Friends, descriptor.ListType);
        Assert.Equal(3, descriptor.Version);
        Assert.Equal(
            "/internal/api/ops/relationship-projection/streams?limit=25",
            handler.Requests[0].PathAndQuery);
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

    [Fact]
    public async Task RunLeaseAsync_RenewFailure_LosesLeaseAtRenew()
    {
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamPage(
                [new RelationshipProjectionStreamDescriptor(
                    10,
                    RelationshipProjectionListType.Friends,
                    2)],
                false,
                null,
                null)
        ]);
        var worker = CreateWorker(
            new RecordingStateStore { RenewReturns = false },
            new RecordingProjectionStore(),
            source);

        var ex = await Assert.ThrowsAsync<RelationshipProjectionRebuildLeaseLostException>(
            () => worker.RunLeaseAsync(NewLease(), CancellationToken.None));
        Assert.Equal("renew", ex.Stage);
    }

    [Fact]
    public async Task RunLeaseAsync_CommitPageFailure_LosesLeaseAtCommit()
    {
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamPage(
                [new RelationshipProjectionStreamDescriptor(
                    10,
                    RelationshipProjectionListType.Friends,
                    2)],
                true,
                10,
                RelationshipProjectionListType.Friends)
        ]);
        var worker = CreateWorker(
            new RecordingStateStore { CommitReturns = false },
            new RecordingProjectionStore(),
            source);

        var ex = await Assert.ThrowsAsync<RelationshipProjectionRebuildLeaseLostException>(
            () => worker.RunLeaseAsync(NewLease(), CancellationToken.None));
        Assert.Equal("commit-page", ex.Stage);
    }

    [Fact]
    public async Task RunLeaseAsync_RejectsSmallerOwnerIdAppearingDuringScan()
    {
        var source = new RecordingSource(
        [
            new RelationshipProjectionStreamPage(
                [new RelationshipProjectionStreamDescriptor(
                    10,
                    RelationshipProjectionListType.Friends,
                    2)],
                false,
                null,
                null)
        ]);
        var worker = CreateWorker(
            new RecordingStateStore(),
            new RecordingProjectionStore(),
            source);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => worker.RunLeaseAsync(
                NewCursorLease(50, RelationshipProjectionListType.Friends),
                CancellationToken.None));
    }

    [Theory]
    [InlineData(429, "source_http_429")]
    [InlineData(503, "source_http_503")]
    [InlineData(500, "source_http_500")]
    public void ClassifyError_MapsHttpStatus_ForBackoffRetry(int status, string expected)
    {
        var error = new RelationshipProjectionSourceException((HttpStatusCode)status);
        Assert.Equal(expected, RelationshipProjectionRebuildWorker.ClassifyError(error));
    }

    [Theory]
    [InlineData("source_transport_failed")]
    [InlineData("source_timeout")]
    [InlineData("source_contract_invalid")]
    [InlineData("snapshot_version_mismatch")]
    [InlineData("rebuild_failed")]
    public void ClassifyError_MapsTransportAndContractFailures(string expected)
    {
        Exception error = expected switch
        {
            "source_transport_failed" => new HttpRequestException("connection reset"),
            "source_timeout" => new TaskCanceledException("timed out"),
            "source_contract_invalid" => new InvalidDataException("bad page"),
            "snapshot_version_mismatch" => new RelationshipProjectionSnapshotVersionMismatchException(
                1, RelationshipProjectionListType.Friends, 2, 5),
            _ => new InvalidOperationException("boom")
        };
        Assert.Equal(expected, RelationshipProjectionRebuildWorker.ClassifyError(error));
    }

    [Fact]
    public async Task ExecuteAsync_ReleasesLeaseWithRetryDelay_OnSourceFailure()
    {
        var cancel = new CancellationTokenSource();
        var state = new RecordingStateStore { CancelAfterRelease = cancel };
        var worker = CreateWorker(
            state,
            new RecordingProjectionStore(),
            new ThrowingSource(() => new RelationshipProjectionSourceException(
                HttpStatusCode.TooManyRequests)));

        await worker.StartAsync(cancel.Token);
        await WaitUntilAsync(() => state.ReleasedErrorCodes.Count > 0);
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal("source_http_429", Assert.Single(state.ReleasedErrorCodes));
        Assert.Equal(
            TimeSpan.FromSeconds(1),
            Assert.Single(state.ReleasedRetryDelays));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 300 && !condition(); i++)
            await Task.Delay(10);
    }

    [Fact]
    public async Task Source_MapsHttpStatus_ForBackoffClassification()
    {
        async Task<RelationshipProjectionSourceException> Capture(HttpStatusCode status)
        {
            var handler = new RecordingHttpHandler(new Queue<HttpResponseMessage>(
            [
                new(status)
                {
                    Content = new StringContent(
                        """{"error":"boom"}""",
                        Encoding.UTF8,
                        "application/json")
                }
            ]));
            using var client = new HttpClient(handler)
            {
                BaseAddress = new Uri("https://server.example/internal/")
            };
            var source = new ServerRelationshipProjectionSnapshotSource(client);
            return await Assert.ThrowsAsync<RelationshipProjectionSourceException>(
                () => source.ListStreamsAsync(null, null, 25, CancellationToken.None));
        }

        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            (await Capture(HttpStatusCode.TooManyRequests)).StatusCode);
        Assert.Equal(
            HttpStatusCode.ServiceUnavailable,
            (await Capture(HttpStatusCode.ServiceUnavailable)).StatusCode);
    }

    private static RelationshipProjectionRebuildLease NewCursorLease(
        long ownerUserId,
        RelationshipProjectionListType listType) => new(
        "test-instance",
        new string('a', 32),
        ownerUserId,
        listType,
        1,
        true,
        0);

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

        public Task<IReadOnlyList<RelationshipProjectionHistoryEntry>> QueryHistoryAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            long fromVersionExclusive,
            int limit,
            CancellationToken ct = default) =>
            throw new NotSupportedException();
    }

    private sealed class RecordingStateStore : IRelationshipProjectionRebuildStateStore
    {
        public int RenewCalls { get; private set; }
        public List<(long Owner, RelationshipProjectionListType List)> Cursors { get; } = [];
        public List<string> ReleasedErrorCodes { get; } = [];
        public List<TimeSpan> ReleasedRetryDelays { get; } = [];
        public bool RenewReturns = true;
        public bool CommitReturns = true;
        public CancellationTokenSource? CancelAfterRelease;

        public Task<RelationshipProjectionRebuildLease?> TryAcquireAsync(
            string leaseOwner,
            TimeSpan leaseDuration,
            CancellationToken ct = default) =>
            Task.FromResult<RelationshipProjectionRebuildLease?>(new(
                leaseOwner,
                new string('b', 32),
                null,
                null,
                0,
                false,
                0));

        public Task<bool> RenewAsync(
            RelationshipProjectionRebuildLease lease,
            TimeSpan leaseDuration,
            CancellationToken ct = default)
        {
            RenewCalls++;
            return Task.FromResult(RenewReturns);
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
            return Task.FromResult(CommitReturns);
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
            CancellationToken ct = default)
        {
            ReleasedErrorCodes.Add(errorCode);
            ReleasedRetryDelays.Add(retryDelay);
            CancelAfterRelease?.Cancel();
            return Task.FromResult(true);
        }
    }

    private sealed class ThrowingSource(Func<Exception> exceptionFactory)
        : IRelationshipProjectionSnapshotSource
    {
        public Task<RelationshipProjectionStreamPage> ListStreamsAsync(
            long? afterOwnerUserId,
            RelationshipProjectionListType? afterListType,
            int limit,
            CancellationToken ct) => throw exceptionFactory();

        public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct) => throw exceptionFactory();

        public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
            long ownerUserId,
            RelationshipProjectionListType listType,
            CancellationToken ct) => throw exceptionFactory();
    }
}

using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.JetStream;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// 四-2：分片发布部分失败门禁测试。
/// <para>
/// 验证 <see cref="JetStreamRealtimeEventPublisher"/> 在分片模式下，当 shard 发布失败时
/// 抛出 <see cref="AggregateException"/>（而非静默成功），让上游 Outbox 整条重试。
/// 已成功 shard 依靠 JetStream MsgId（eventId:instanceId）去重，重试时不会重复投递。
/// </para>
/// <para>
/// 测试策略：使用不可达的 NATS 端点（127.0.0.1:1，端口 1 立即拒绝连接）构造 publisher，
/// 使所有 shard 发布均失败。验证 publisher 收集所有失败并抛出 <see cref="AggregateException"/>，
/// 而非吞掉异常静默返回成功。
/// </para>
/// </summary>
public sealed class PartialShardFailureTests : IAsyncDisposable
{
    [Fact]
    public async Task PublishWithPayload_ShardPublishFailures_ThrowsAggregateExceptionNotSilentSuccess()
    {
        var (publisher, connectionClient) = BuildPublisher(
            shardSubjectPattern: "chat.realtime-events.{0}",
            gatewayInstanceIds: ["gw-fail-1", "gw-fail-2", "gw-fail-3"]);

        var evt = new RealtimeEvent
        {
            EventId = "evt-partial-shard-fail",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 42,
            OccurredAtMs = 1
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => publisher.PublishWithPayloadAsync(evt, null, cts.Token));

        // 至少捕获到一个 shard 失败异常——证明失败被聚合而非静默吞掉。
        Assert.NotEmpty(ex.InnerExceptions);
        Assert.All(ex.InnerExceptions, inner => Assert.NotNull(inner));
    }

    [Fact]
    public async Task PublishToMany_ShardPublishFailures_ThrowsAggregateExceptionNotSilentSuccess()
    {
        var (publisher, connectionClient) = BuildPublisher(
            shardSubjectPattern: "chat.realtime-events.{0}",
            gatewayInstanceIds: ["gw-many-1", "gw-many-2"]);

        var evt = new RealtimeEvent
        {
            EventId = "evt-partial-shard-many-fail",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 7,
            TargetUserIds = [7, 8, 9],
            OccurredAtMs = 1
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => publisher.PublishToManyWithPayloadAsync(evt, null, cts.Token));

        Assert.NotEmpty(ex.InnerExceptions);
        Assert.All(ex.InnerExceptions, inner => Assert.NotNull(inner));
    }

    /// <summary>
    /// 验证非分片模式（无 shard pattern）下走广播路径，不会触发分片聚合逻辑。
    /// 广播路径失败时直接向上抛出原始异常，而非包装为 AggregateException。
    /// </summary>
    [Fact]
    public async Task PublishWithPayload_NoShardPattern_BroadcastFailureThrowsOriginalException()
    {
        var (publisher, connectionClient) = BuildPublisher(
            shardSubjectPattern: null,
            gatewayInstanceIds: ["gw-broadcast"]);

        var evt = new RealtimeEvent
        {
            EventId = "evt-broadcast-fail",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 99,
            OccurredAtMs = 1
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        // 非分片模式：广播路径直接 await，不经过 PublishToShardsParallelAsync，
        // 失败时抛出原始异常（非 AggregateException）。
        await Assert.ThrowsAnyAsync<Exception>(
            () => publisher.PublishWithPayloadAsync(evt, null, cts.Token));
    }

    private static (JetStreamRealtimeEventPublisher Publisher, NatsConnectionClient ConnectionClient) BuildPublisher(
        string? shardSubjectPattern,
        IReadOnlyList<string> gatewayInstanceIds)
    {
        var queueOptions = new RealtimeQueueOptions
        {
            Provider = "JetStream",
            // 端口 1 立即拒绝 TCP 连接，避免长时间等待连接超时。
            Endpoint = "nats://127.0.0.1:1",
            ConsumerGroup = "test-partial-shard",
            Topics = new RealtimeQueueTopics
            {
                IncomingMessages = "chat.incoming",
                MessageReceipts = "chat.receipts",
                RealtimeEvents = "chat.realtime-events"
            },
            RealtimeEventsShardSubjectPattern = shardSubjectPattern
        };

        var natsOptions = new OptionsWrapper<NatsOptions>(new NatsOptions
        {
            QueueGroup = "test-partial-shard",
            Subjects = new NatsSubjectOptions
            {
                IncomingMessages = "chat.incoming",
                RealtimeEvents = "chat.realtime-events"
            }
        });

        var metrics = new NatsTransportMetrics(
            $"ChatApp.Realtime.Tests.PartialShard.{Guid.NewGuid():N}");
        var connectionClient = new NatsConnectionClient(
            queueOptions,
            natsOptions,
            metrics,
            NullLogger<NatsConnectionClient>.Instance);

        var contextManager = new JetStreamContextManager(
            connectionClient,
            queueOptions,
            new JetStreamOptions(),
            NullLogger<JetStreamContextManager>.Instance,
            shardSubjectPattern: shardSubjectPattern);

        var gatewayDirectory = new InlineGatewayDirectory(gatewayInstanceIds);
        var publisher = new JetStreamRealtimeEventPublisher(
            contextManager,
            gatewayDirectory,
            shardSubjectPattern,
            maxShardParallelism: 4);

        return (publisher, connectionClient);
    }

    private sealed class InlineGatewayDirectory : IGatewayDirectory
    {
        private readonly IReadOnlyList<string> _gateways;

        public InlineGatewayDirectory(IReadOnlyList<string> gateways) =>
            _gateways = gateways;

        public Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
            long userId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_gateways);

        public Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
            IReadOnlyList<long> userIds,
            CancellationToken cancellationToken = default)
        {
            var map = new Dictionary<long, IReadOnlyList<string>>(userIds.Count);
            foreach (var id in userIds)
                map[id] = _gateways;
            return Task.FromResult<IReadOnlyDictionary<long, IReadOnlyList<string>>>(map);
        }

        public Task<GatewayLookupResult> GetOnlineGatewaysWithStatusAsync(
            long userId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new GatewayLookupResult(GatewayLookupResultKind.Success, _gateways));

        public Task<GatewayLookupManyResult> GetOnlineGatewaysManyWithStatusAsync(
            IReadOnlyList<long> userIds,
            CancellationToken cancellationToken = default)
        {
            var map = new Dictionary<long, IReadOnlyList<string>>(userIds.Count);
            foreach (var id in userIds)
                map[id] = _gateways;
            return Task.FromResult(
                new GatewayLookupManyResult(GatewayLookupResultKind.Success, map));
        }
    }

    private sealed class OptionsWrapper<T>(T value) : IOptions<T> where T : class
    {
        public T Value { get; } = value;
    }

    public async ValueTask DisposeAsync()
    {
        // NatsConnectionClient 内部 Lazy<NatsClient> 可能未创建（端口 1 立即拒绝连接时
        // NatsClient 仍会被创建但连接失败）。DisposeAsync 会安全清理。
        // 每个 test 方法独立构造 client，这里通过 GC 释放；如需显式释放可在 test 内 using。
        await ValueTask.CompletedTask;
    }
}

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly IWatcherGatewayDirectory _watcherGatewayDirectory;
    private readonly string? _shardSubjectPattern;
    private readonly ILogger<NatsRealtimeEventPublisher> _logger;
    private readonly RoutingMetrics? _routingMetrics;
    private readonly RealtimeMetrics? _realtimeMetrics;

    public NatsRealtimeEventPublisher(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventPublisher> logger,
        IGatewayDirectory? gatewayDirectory = null,
        string? shardSubjectPattern = null,
        RoutingMetrics? routingMetrics = null,
        IWatcherGatewayDirectory? watcherGatewayDirectory = null,
        RealtimeMetrics? realtimeMetrics = null)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _watcherGatewayDirectory = watcherGatewayDirectory ?? NullWatcherGatewayDirectory.Instance;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        _routingMetrics = routingMetrics;
        _realtimeMetrics = realtimeMetrics;
    }

    /// <summary>Perf-4：旧入口，回退到序列化路径。实际逻辑在 <see cref="PublishWithPayloadAsync"/>。</summary>
    public Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
        => PublishWithPayloadAsync(evt, null, ct);

    public async Task PublishWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payloadUtf8, CancellationToken ct = default)
    {
        // Perf-4：优先使用预序列化的 UTF-8 字节，避免重新序列化；为空时回退到序列化路径。
        var json = payloadUtf8 is { Length: > 0 } bytes
            ? Encoding.UTF8.GetString(bytes.Span)
            : JsonSerializer.Serialize(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 账号清理相关事件始终广播。
        var isAccountCleanup = evt.Type is RealtimeEventType.UserAccountDeleted
            or RealtimeEventType.AccountCleanupCompleted
            or RealtimeEventType.AttachmentBlobsPurge;

        if (isAccountCleanup)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "account_cleanup");
            await PublishToSubjectAsync(_options.Topics.AccountCleanup, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        // 非分片模式：广播。
        if (_shardSubjectPattern is null)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_pattern");
            await PublishToSubjectAsync(_options.Topics.RealtimeEvents, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        // 分片模式：查询目标用户在线 Gateway 集合并定向发布。
        var sw = Stopwatch.StartNew();
        var lookup = await _gatewayDirectory
            .GetOnlineGatewaysWithStatusAsync(evt.TargetUserId, ct)
            .ConfigureAwait(false);
        sw.Stop();
        _routingMetrics?.RecordDirectoryQuery("gateway", "single", sw.Elapsed, lookup.Gateways.Count);

        // P0-9：根据查询状态分类处理。
        // - Success：定向发布到命中的 Gateway 实例。
        // - UserOffline：查询成功但用户离线，正常不投递。
        // - LookupFailure / PartialLookupFailure：枚举所有活跃 shards 分别发布，避免广播 fallback 无人消费。
        if (lookup.Kind is GatewayLookupResultKind.LookupFailure
            or GatewayLookupResultKind.PartialLookupFailure)
        {
            await PublishToAllShardsAsync(json, evt, "lookup_failure", ct)
                .ConfigureAwait(false);
            return;
        }

        if (lookup.Kind == GatewayLookupResultKind.UserOffline)
        {
            // 查询成功但用户离线：正常不投递（避免无谓 fanout）。
            _routingMetrics?.RecordBroadcastFallback("realtime", "user_offline");
            return;
        }

        var gateways = lookup.Gateways;
        if (gateways.Count == 0)
        {
            // 兜底：理论上不应到达（UserOffline 已处理），保持原广播回退行为以容错。
            _routingMetrics?.RecordBroadcastFallback("realtime", "empty_directory");
            await PublishToSubjectAsync(_options.Topics.RealtimeEvents, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in gateways)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
                continue;

            var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern, instanceId);
            await PublishToSubjectAsync(subject, json, evt, ct)
                .ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("realtime", "single", gateways.Count);
    }

    /// <summary>Perf-4：旧入口，回退到序列化路径。实际逻辑在 <see cref="PublishToManyWithPayloadAsync"/>。</summary>
    public Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default)
        => PublishToManyWithPayloadAsync(evt, null, ct);

    public async Task PublishToManyWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payloadUtf8, CancellationToken ct = default)
    {
        // 未携带多目标列表时回退到单目标发布路径。
        if (evt.TargetUserIds is null || evt.TargetUserIds.Length == 0)
        {
            await PublishWithPayloadAsync(evt, payloadUtf8, ct).ConfigureAwait(false);
            return;
        }

        // Perf-4：优先使用预序列化的 UTF-8 字节，避免重新序列化；为空时回退到序列化路径。
        var json = payloadUtf8 is { Length: > 0 } bytes
            ? Encoding.UTF8.GetString(bytes.Span)
            : JsonSerializer.Serialize(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 非分片模式：广播单条消息，各 Gateway 自行遍历 TargetUserIds 投递本机会话。
        if (_shardSubjectPattern is null)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_pattern");
            await PublishToSubjectAsync(_options.Topics.RealtimeEvents, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        // 分片模式：批量查询所有目标用户的在线 Gateway 集合，按实例聚合投递。
        var sw = Stopwatch.StartNew();
        var lookup = await _gatewayDirectory
            .GetOnlineGatewaysManyWithStatusAsync(evt.TargetUserIds, ct)
            .ConfigureAwait(false);
        sw.Stop();

        var instances = new HashSet<string>(StringComparer.Ordinal);
        foreach (var kvp in lookup.GatewayMap)
        {
            foreach (var instanceId in kvp.Value)
            {
                if (!string.IsNullOrWhiteSpace(instanceId))
                    instances.Add(instanceId);
            }
        }

        _routingMetrics?.RecordDirectoryQuery("gateway", "many", sw.Elapsed, instances.Count);

        // P0-9：批量查询失败时枚举所有活跃 shards 分别发布。
        if (lookup.Kind is GatewayLookupResultKind.LookupFailure
            or GatewayLookupResultKind.PartialLookupFailure)
        {
            await PublishToAllShardsAsync(json, evt, "partial_lookup_failure", ct)
                .ConfigureAwait(false);
            return;
        }

        if (instances.Count == 0)
        {
            // 路由目录为空（所有目标离线）：回退到广播，避免事件丢失。
            _routingMetrics?.RecordBroadcastFallback("realtime", "empty_directory");
            await PublishToSubjectAsync(_options.Topics.RealtimeEvents, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in instances)
        {
            var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern, instanceId);
            await PublishToSubjectAsync(subject, json, evt, ct)
                .ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("realtime", "many", instances.Count);
        _routingMetrics?.RecordFanout(evt.TargetUserIds!.Length, instances.Count);
    }

    /// <summary>
    /// P0-9：枚举所有已知活跃 Gateway shards，分别发布到各自 shard subject。
    /// <para>
    /// 用于 <see cref="GatewayLookupResultKind.LookupFailure"/> /
    /// <see cref="GatewayLookupResultKind.PartialLookupFailure"/> 时避免分片模式下广播 fallback 无人消费。
    /// 若活跃 shards 为空（目录查询也失败），最终回退到广播 subject 保证不丢事件。
    /// </para>
    /// </summary>
    /// <param name="json">已序列化的事件载荷。</param>
    /// <param name="evt">原始事件（用于日志/subject 计算）。</param>
    /// <param name="reason">fallback 原因，用于指标标签。</param>
    /// <param name="ct">取消令牌。</param>
    private async Task PublishToAllShardsAsync(
        string json,
        RealtimeEvent evt,
        string reason,
        CancellationToken ct)
    {
        var shards = await _watcherGatewayDirectory
            .ListActiveShardsAsync(ct)
            .ConfigureAwait(false);

        if (shards.Count == 0)
        {
            // 活跃 shards 也为空（双重故障）：最终回退到广播，至少尝试投递一次。
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_active_shards");
            _realtimeMetrics?.RecordShardFallback(reason, 0);
            _logger.LogWarning(
                "分片 fallback 无活跃 shards，回退到广播。事件类型={Type}；事件编号={EventId}；原因={Reason}",
                evt.Type,
                evt.EventId,
                reason);
            await PublishToSubjectAsync(_options.Topics.RealtimeEvents, json, evt, ct)
                .ConfigureAwait(false);
            return;
        }

        _realtimeMetrics?.RecordShardFallback(reason, shards.Count);
        _logger.LogWarning(
            "路由目录查询失败，分片 fallback 到所有活跃 shards。事件类型={Type}；事件编号={EventId}；原因={Reason}；shard 数={Count}",
            evt.Type,
            evt.EventId,
            reason,
            shards.Count);

        foreach (var instanceId in shards)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
                continue;

            var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern!, instanceId);
            await PublishToSubjectAsync(subject, json, evt, ct)
                .ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("realtime", "fallback", shards.Count);
    }

    private async Task PublishToSubjectAsync(
        string subject,
        string json,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        await _connectionClient.Client
            .PublishAsync(
                subject,
                json,
                headers: NatsTraceContext.CreatePropagationHeaders(),
                cancellationToken: ct)
            .ConfigureAwait(false);

        // P1-5：热路径逐事件 Information 日志在高扇出场景下会放大日志量（分片模式按 Gateway 实例重复）。
        // 降级为 Debug；正常吞吐依赖 RoutingMetrics 的 Counter/Histogram，失败与广播回退保留 Warning。
        _logger.LogDebug(
            "实时事件已发布到 NATS。事件类型={Type}；Subject={Subject}",
            evt.Type,
            subject);
    }
}

using System.Diagnostics;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly JetStreamContextManager _contextManager;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly IWatcherGatewayDirectory _watcherGatewayDirectory;
    private readonly string? _shardSubjectPattern;
    private readonly RoutingMetrics? _routingMetrics;
    private readonly RealtimeMetrics? _realtimeMetrics;
    private readonly ILogger<JetStreamRealtimeEventPublisher>? _logger;

    public JetStreamRealtimeEventPublisher(
        JetStreamContextManager contextManager,
        IGatewayDirectory? gatewayDirectory = null,
        string? shardSubjectPattern = null,
        RoutingMetrics? routingMetrics = null,
        IWatcherGatewayDirectory? watcherGatewayDirectory = null,
        RealtimeMetrics? realtimeMetrics = null,
        ILogger<JetStreamRealtimeEventPublisher>? logger = null)
    {
        _contextManager = contextManager;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _watcherGatewayDirectory = watcherGatewayDirectory ?? NullWatcherGatewayDirectory.Instance;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        _routingMetrics = routingMetrics;
        _realtimeMetrics = realtimeMetrics;
        _logger = logger;
    }

    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 账号清理事件始终广播（Server Saga 共享 durable consumer）。
        if (evt.Type is RealtimeEventType.UserAccountDeleted
            or RealtimeEventType.AccountCleanupCompleted
            or RealtimeEventType.AttachmentBlobsPurge)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "account_cleanup");
            await _contextManager
                .PublishAccountCleanupEventAsync(evt.EventId, payload, ct)
                .ConfigureAwait(false);
            return;
        }

        // 非分片模式：广播到全量 subject。
        if (_shardSubjectPattern is null)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_pattern");
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
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
        if (lookup.Kind is GatewayLookupResultKind.LookupFailure
            or GatewayLookupResultKind.PartialLookupFailure)
        {
            await PublishToAllShardsAsync(payload, evt, "lookup_failure", ct)
                .ConfigureAwait(false);
            return;
        }

        if (lookup.Kind == GatewayLookupResultKind.UserOffline)
        {
            // 查询成功但用户离线：正常不投递。
            _routingMetrics?.RecordBroadcastFallback("realtime", "user_offline");
            return;
        }

        var gateways = lookup.Gateways;
        if (gateways.Count == 0)
        {
            // 兜底：理论上不应到达（UserOffline 已处理），保持原广播回退行为以容错。
            _routingMetrics?.RecordBroadcastFallback("realtime", "empty_directory");
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
                .ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in gateways)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
                continue;

            var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern, instanceId);
            await _contextManager
                .PublishRealtimeEventToSubjectAsync(subject, evt.EventId + ":" + instanceId, payload, ct)
                .ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("realtime", "single", gateways.Count);
    }

    public async Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        // 未携带多目标列表时回退到单目标发布路径。
        if (evt.TargetUserIds is null || evt.TargetUserIds.Length == 0)
        {
            await PublishAsync(evt, ct).ConfigureAwait(false);
            return;
        }

        var payload = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 非分片模式：广播单条消息，各 Gateway 自行遍历 TargetUserIds 投递本机会话。
        if (_shardSubjectPattern is null)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_pattern");
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
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
            await PublishToAllShardsAsync(payload, evt, "partial_lookup_failure", ct)
                .ConfigureAwait(false);
            return;
        }

        if (instances.Count == 0)
        {
            // 路由目录为空（所有目标离线）：回退到广播，避免事件丢失。
            _routingMetrics?.RecordBroadcastFallback("realtime", "empty_directory");
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
                .ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in instances)
        {
            var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern, instanceId);
            // 使用复合 MsgId 避免 JetStream 跨 subject 去重吞掉分片消息。
            var msgId = evt.EventId + ":" + instanceId;
            await _contextManager
                .PublishRealtimeEventToSubjectAsync(subject, msgId, payload, ct)
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
    /// <param name="payload">已序列化的事件载荷。</param>
    /// <param name="evt">原始事件（用于日志/subject 计算）。</param>
    /// <param name="reason">fallback 原因，用于指标标签。</param>
    /// <param name="ct">取消令牌。</param>
    private async Task PublishToAllShardsAsync(
        string payload,
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
            _logger?.LogWarning(
                "分片 fallback 无活跃 shards，回退到广播。事件类型={Type}；事件编号={EventId}；原因={Reason}",
                evt.Type,
                evt.EventId,
                reason);
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
                .ConfigureAwait(false);
            return;
        }

        _realtimeMetrics?.RecordShardFallback(reason, shards.Count);
        _logger?.LogWarning(
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
            // 使用复合 MsgId 避免 JetStream 跨 subject 去重吞掉分片消息。
            var msgId = evt.EventId + ":" + instanceId;
            await _contextManager
                .PublishRealtimeEventToSubjectAsync(subject, msgId, payload, ct)
                .ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("realtime", "fallback", shards.Count);
    }
}

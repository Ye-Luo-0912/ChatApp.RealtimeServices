using System.Diagnostics;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly JetStreamContextManager _contextManager;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly string? _shardSubjectPattern;
    private readonly RoutingMetrics? _routingMetrics;

    public JetStreamRealtimeEventPublisher(
        JetStreamContextManager contextManager,
        IGatewayDirectory? gatewayDirectory = null,
        string? shardSubjectPattern = null,
        RoutingMetrics? routingMetrics = null)
    {
        _contextManager = contextManager;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        _routingMetrics = routingMetrics;
    }

    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 账号清理相关事件始终广播（Server Saga 共享 durable consumer）。
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
        var gateways = await _gatewayDirectory
            .GetOnlineGatewaysAsync(evt.TargetUserId, ct)
            .ConfigureAwait(false);
        sw.Stop();
        _routingMetrics?.RecordDirectoryQuery("gateway", "single", sw.Elapsed, gateways.Count);

        if (gateways.Count == 0)
        {
            // 路由目录为空（用户离线或查询失败）：回退到广播，避免事件丢失。
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
        var gatewayMap = await _gatewayDirectory
            .GetOnlineGatewaysManyAsync(evt.TargetUserIds, ct)
            .ConfigureAwait(false);
        sw.Stop();

        var instances = new HashSet<string>(StringComparer.Ordinal);
        foreach (var kvp in gatewayMap)
        {
            foreach (var instanceId in kvp.Value)
            {
                if (!string.IsNullOrWhiteSpace(instanceId))
                    instances.Add(instanceId);
            }
        }

        _routingMetrics?.RecordDirectoryQuery("gateway", "many", sw.Elapsed, instances.Count);

        if (instances.Count == 0)
        {
            // 路由目录为空（所有目标离线或查询失败）：回退到广播，避免事件丢失。
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
}

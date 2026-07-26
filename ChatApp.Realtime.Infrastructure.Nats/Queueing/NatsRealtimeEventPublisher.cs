using System.Diagnostics;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly string? _shardSubjectPattern;
    private readonly ILogger<NatsRealtimeEventPublisher> _logger;
    private readonly RoutingMetrics? _routingMetrics;

    public NatsRealtimeEventPublisher(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventPublisher> logger,
        IGatewayDirectory? gatewayDirectory = null,
        string? shardSubjectPattern = null,
        RoutingMetrics? routingMetrics = null)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        _routingMetrics = routingMetrics;
    }

    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(
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
        var gateways = await _gatewayDirectory
            .GetOnlineGatewaysAsync(evt.TargetUserId, ct)
            .ConfigureAwait(false);
        sw.Stop();
        _routingMetrics?.RecordDirectoryQuery("gateway", "single", sw.Elapsed, gateways.Count);

        if (gateways.Count == 0)
        {
            // 回退到广播。
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

    public async Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        // 未携带多目标列表时回退到单目标发布路径。
        if (evt.TargetUserIds is null || evt.TargetUserIds.Length == 0)
        {
            await PublishAsync(evt, ct).ConfigureAwait(false);
            return;
        }

        var json = JsonSerializer.Serialize(
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

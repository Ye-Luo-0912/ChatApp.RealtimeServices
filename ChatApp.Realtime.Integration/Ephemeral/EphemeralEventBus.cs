using System.Diagnostics;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Serialization;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>
/// 临时事件（Typing / Presence）总线：
/// <para>- 按 Gateway 分片定向投递（Sharded 模式）或广播（Broadcast 模式）。</para>
/// <para>- NATS Core 发布（非 JetStream / 非 Outbox）。</para>
/// <para>- 全量订阅本实例所属分片或全量 subject。</para>
/// <para>- 承担 Presence 鉴权 request/reply 服务端循环。</para>
/// </summary>
internal sealed class EphemeralEventBus
{
    private readonly NatsConnectionProvider _connectionProvider;
    private readonly RealtimeIntegrationOptions _options;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly IWatcherGatewayDirectory _watcherGatewayDirectory;
    private readonly RoutingMetrics? _routingMetrics;
    private readonly ILogger _logger;

    public EphemeralEventBus(
        NatsConnectionProvider connectionProvider,
        RealtimeIntegrationOptions options,
        IGatewayDirectory gatewayDirectory,
        IWatcherGatewayDirectory watcherGatewayDirectory,
        RoutingMetrics? routingMetrics,
        ILogger logger)
    {
        _connectionProvider = connectionProvider;
        _options = options;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _watcherGatewayDirectory = watcherGatewayDirectory ?? NullWatcherGatewayDirectory.Instance;
        _routingMetrics = routingMetrics;
        _logger = logger;
    }

    /// <summary>
    /// 是否启用按 Gateway 分片投递。
    /// </summary>
    private bool IsShardedRoutingEnabled =>
        _options.RoutingMode is EventRoutingMode.Sharded
        && ShardedSubjectFormatter.IsSharded(_options.RealtimeEventsShardSubjectPattern);

    /// <summary>
    /// 当前实例订阅的 Ephemeral Typing 分片 subject。
    /// </summary>
    private string EphemeralTypingShardSubject =>
        ShardedSubjectFormatter.Format(_options.EphemeralTypingShardSubjectPattern, _options.InstanceId);

    /// <summary>
    /// 当前实例订阅的 Ephemeral Presence 分片 subject。
    /// </summary>
    private string EphemeralPresenceShardSubject =>
        ShardedSubjectFormatter.Format(_options.EphemeralPresenceShardSubjectPattern, _options.InstanceId);

    public async Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);

        // 非分片模式或目标用户无效：广播。
        if (!IsShardedRoutingEnabled || evt.TargetUserId <= 0)
        {
            _routingMetrics?.RecordBroadcastFallback("typing", "no_pattern");
            await PublishEphemeralTypingToSubjectAsync(
                _options.EphemeralTypingSubject,
                evt,
                ct).ConfigureAwait(false);
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
            // 路由目录为空：回退到广播，保证至少有一次投递尝试。
            _routingMetrics?.RecordBroadcastFallback("typing", "empty_directory");
            await PublishEphemeralTypingToSubjectAsync(
                _options.EphemeralTypingSubject,
                evt,
                ct).ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in gateways)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
                continue;

            var shardSubject = ShardedSubjectFormatter.Format(
                _options.EphemeralTypingShardSubjectPattern,
                instanceId);
            await PublishEphemeralTypingToSubjectAsync(
                shardSubject,
                evt,
                ct).ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("typing", "single", gateways.Count);
    }

    private async Task PublishEphemeralTypingToSubjectAsync(
        string subject,
        EphemeralTypingEvent evt,
        CancellationToken ct)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "ephemeral_typing.publish",
            subject);
        try
        {
            await _connectionProvider.Client.PublishAsync(
                    subject,
                    RealtimeWireSerializer.Serialize(evt),
                    cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);

        // 非分片模式或被观察用户无效：广播。
        if (!IsShardedRoutingEnabled || evt.UserId <= 0)
        {
            _routingMetrics?.RecordBroadcastFallback("presence", "no_pattern");
            await PublishEphemeralPresenceToSubjectAsync(
                _options.EphemeralPresenceSubject,
                evt,
                ct).ConfigureAwait(false);
            return;
        }

        // 分片模式：查询有哪些 Gateway 实例的本地用户正在观察此用户，定向投递。
        var sw = Stopwatch.StartNew();
        var gateways = await _watcherGatewayDirectory
            .GetWatcherGatewaysAsync(evt.UserId, ct)
            .ConfigureAwait(false);
        sw.Stop();
        _routingMetrics?.RecordDirectoryQuery("watcher", "single", sw.Elapsed, gateways.Count);

        if (gateways.Count == 0)
        {
            // 观察者目录为空（无人观察或查询失败）：回退到广播，保证至少有一次投递尝试。
            _routingMetrics?.RecordBroadcastFallback("presence", "empty_directory");
            await PublishEphemeralPresenceToSubjectAsync(
                _options.EphemeralPresenceSubject,
                evt,
                ct).ConfigureAwait(false);
            return;
        }

        foreach (var instanceId in gateways)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
                continue;

            var shardSubject = ShardedSubjectFormatter.Format(
                _options.EphemeralPresenceShardSubjectPattern,
                instanceId);
            await PublishEphemeralPresenceToSubjectAsync(
                shardSubject,
                evt,
                ct).ConfigureAwait(false);
        }

        _routingMetrics?.RecordShardPublish("presence", "single", gateways.Count);
    }

    private async Task PublishEphemeralPresenceToSubjectAsync(
        string subject,
        EphemeralPresenceEvent evt,
        CancellationToken ct)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "ephemeral_presence.publish",
            subject);
        try
        {
            await _connectionProvider.Client.PublishAsync(
                    subject,
                    RealtimeWireSerializer.Serialize(evt),
                    cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // 分片模式：订阅本实例专属 subject；广播模式：订阅全量 subject。
        var subject = IsShardedRoutingEnabled
            ? EphemeralTypingShardSubject
            : _options.EphemeralTypingSubject;

        await foreach (var msg in _connectionProvider.Client.SubscribeAsync<string>(
                           subject,
                           cancellationToken: ct)
                       .ConfigureAwait(false))
        {
            if (string.IsNullOrWhiteSpace(msg.Data))
                continue;

            EphemeralTypingEvent? evt;
            try
            {
                evt = RealtimeWireSerializer.DeserializeEphemeralTyping(msg.Data);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Ephemeral Typing 反序列化失败");
                continue;
            }

            if (evt is not null)
                yield return evt;
        }
    }

    public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // 分片模式：订阅本实例专属 subject；广播模式：订阅全量 subject。
        var subject = IsShardedRoutingEnabled
            ? EphemeralPresenceShardSubject
            : _options.EphemeralPresenceSubject;

        await foreach (var msg in _connectionProvider.Client.SubscribeAsync<string>(
                           subject,
                           cancellationToken: ct)
                       .ConfigureAwait(false))
        {
            if (string.IsNullOrWhiteSpace(msg.Data))
                continue;

            EphemeralPresenceEvent? evt;
            try
            {
                evt = RealtimeWireSerializer.DeserializeEphemeralPresence(msg.Data);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Ephemeral Presence 反序列化失败");
                continue;
            }

            if (evt is not null)
                yield return evt;
        }
    }

    public async Task ServePresenceAuthorizeAsync(
        Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        await foreach (var msg in _connectionProvider.Client.SubscribeAsync<string>(
                           _options.PresenceAuthorizeSubject,
                           cancellationToken: ct)
                       .ConfigureAwait(false))
        {
            if (string.IsNullOrWhiteSpace(msg.Data) || string.IsNullOrWhiteSpace(msg.ReplyTo))
                continue;

            PresenceAuthorizeQuery? query;
            try
            {
                query = RealtimeWireSerializer.DeserializePresenceAuthorizeQuery(msg.Data);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PresenceAuthorize 请求反序列化失败");
                continue;
            }

            if (query is null)
                continue;

            PresenceAuthorizeResponse response;
            try
            {
                response = await handler(query, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "PresenceAuthorize handler 失败 Watcher={Watcher}", query.WatcherUserId);
                response = new PresenceAuthorizeResponse { AllowedUserIds = [] };
            }

            try
            {
                await _connectionProvider.Client.PublishAsync(
                        msg.ReplyTo,
                        RealtimeWireSerializer.Serialize(response),
                        cancellationToken: ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "PresenceAuthorize 回复失败");
            }
        }
    }
}

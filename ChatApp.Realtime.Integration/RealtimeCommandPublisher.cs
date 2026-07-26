using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.JetStream;
using ChatApp.Realtime.Integration.Serialization;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// Realtime 命令发布器：
/// <para>- 入站消息 / 消息回执：JetStream 持久化发布（带身份头与去重 MsgId）。</para>
/// <para>- Realtime Event：按账号清理事件 / 分片路由 / 广播回退策略发布到 JetStream。</para>
/// </summary>
internal sealed class RealtimeCommandPublisher
{
    private readonly JetStreamTopologyManager _topology;
    private readonly RealtimeIntegrationOptions _options;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly RoutingMetrics? _routingMetrics;

    public RealtimeCommandPublisher(
        JetStreamTopologyManager topology,
        RealtimeIntegrationOptions options,
        IGatewayDirectory gatewayDirectory,
        RoutingMetrics? routingMetrics)
    {
        _topology = topology;
        _options = options;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _routingMetrics = routingMetrics;
    }

    /// <summary>
    /// 是否启用按 Gateway 分片投递 Realtime Event / Ephemeral Typing。
    /// </summary>
    private bool IsShardedRoutingEnabled =>
        _options.RoutingMode is EventRoutingMode.Sharded
        && ShardedSubjectFormatter.IsSharded(_options.RealtimeEventsShardSubjectPattern);

    /// <summary>
    /// Realtime Event 分片通配符 subject。
    /// </summary>
    private string RealtimeEventsShardWildcard =>
        ShardedSubjectFormatter.ToWildcard(_options.RealtimeEventsShardSubjectPattern);

    public async Task PublishIncomingMessageAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "incoming_message.publish",
            _options.IncomingMessagesSubject);
        try
        {
            await _topology.EnsureStreamAsync(
                _options.IncomingMessagesStream,
                _options.IncomingMessagesSubject,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);
            await _topology.PublishJetStreamWithReconnectRetryAsync(
                _options.IncomingMessagesSubject,
                RealtimeWireSerializer.Serialize(command),
                CreateMessageId(command.SenderUserId, command.ClientMessageId),
                RealtimeIntegrationTelemetry.CreateIdentityHeaders(
                    command.SenderUserId,
                    command.SenderSessionId),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task PublishMessageReceiptAsync(
        MessageReceiptCommand command,
        CancellationToken ct = default)
    {
        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "message_receipt.publish",
            _options.MessageReceiptsSubject);
        try
        {
            await _topology.EnsureStreamAsync(
                _options.MessageReceiptsStream,
                _options.MessageReceiptsSubject,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);
            await _topology.PublishJetStreamWithReconnectRetryAsync(
                _options.MessageReceiptsSubject,
                RealtimeWireSerializer.Serialize(command),
                CreateMessageId(command.ReceiverUserId, command.CommandId),
                RealtimeIntegrationTelemetry.CreateIdentityHeaders(
                    command.ReceiverUserId,
                    command.ReceiverSessionId),
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        // 账号清理事件始终广播（Server Saga 共享 durable consumer）。
        var isAccountCleanup = evt.Type is RealtimeEventType.UserAccountDeleted
            or RealtimeEventType.AccountCleanupCompleted
            or RealtimeEventType.AttachmentBlobsPurge;
        var subject = isAccountCleanup
            ? _options.AccountCleanupSubject
            : _options.RealtimeEventsSubject;

        using var activity = RealtimeIntegrationTelemetry.StartProducer(
            "realtime_event.publish",
            subject);
        try
        {
            // 账号清理事件或非分片模式：广播到全量 subject。
            if (isAccountCleanup || !IsShardedRoutingEnabled)
            {
                _routingMetrics?.RecordBroadcastFallback(
                    "realtime",
                    isAccountCleanup ? "account_cleanup" : "no_pattern");
                await _topology.EnsureStreamAsync(
                    _options.RealtimeEventsStream,
                    subject,
                    _options.MaxAgeHours,
                    ct).ConfigureAwait(false);
                await _topology.PublishJetStreamWithReconnectRetryAsync(
                    subject,
                    RealtimeWireSerializer.Serialize(evt),
                    evt.EventId,
                    RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
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
                // 路由目录为空（用户离线或查询失败）：回退到广播，避免事件丢失。
                _routingMetrics?.RecordBroadcastFallback("realtime", "empty_directory");
                await _topology.EnsureStreamAsync(
                    _options.RealtimeEventsStream,
                    subject,
                    _options.MaxAgeHours,
                    ct).ConfigureAwait(false);
                await _topology.PublishJetStreamWithReconnectRetryAsync(
                    subject,
                    RealtimeWireSerializer.Serialize(evt),
                    evt.EventId,
                    RealtimeIntegrationTelemetry.CreatePropagationHeaders(),
                    ct).ConfigureAwait(false);
                return;
            }

            await _topology.EnsureStreamAsync(
                _options.RealtimeEventsStream,
                RealtimeEventsShardWildcard,
                _options.MaxAgeHours,
                ct).ConfigureAwait(false);

            var payload = RealtimeWireSerializer.Serialize(evt);
            var headers = RealtimeIntegrationTelemetry.CreatePropagationHeaders();
            foreach (var instanceId in gateways)
            {
                if (string.IsNullOrWhiteSpace(instanceId))
                    continue;

                var shardSubject = ShardedSubjectFormatter.Format(
                    _options.RealtimeEventsShardSubjectPattern,
                    instanceId);
                await _topology.PublishJetStreamWithReconnectRetryAsync(
                    shardSubject,
                    payload,
                    evt.EventId + ":" + instanceId,
                    headers,
                    ct).ConfigureAwait(false);
            }

            _routingMetrics?.RecordShardPublish("realtime", "single", gateways.Count);
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    private static string CreateMessageId(long senderUserId, string clientMessageId)
    {
        var bytes = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }
}

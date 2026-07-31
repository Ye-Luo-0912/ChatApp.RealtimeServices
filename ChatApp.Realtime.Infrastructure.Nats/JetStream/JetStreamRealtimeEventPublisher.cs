using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly JetStreamContextManager _contextManager;
    private readonly IGatewayDirectory _gatewayDirectory;
    private readonly IConversationGatewayDirectory _conversationGatewayDirectory;
    private readonly IWatcherGatewayDirectory _watcherGatewayDirectory;
    private readonly string? _shardSubjectPattern;
    private readonly int _maxShardParallelism;
    private readonly RoutingMetrics? _routingMetrics;
    private readonly RealtimeMetrics? _realtimeMetrics;
    private readonly ILogger<JetStreamRealtimeEventPublisher>? _logger;
    private readonly IRealtimeMessageBus? _messageBus;

    public JetStreamRealtimeEventPublisher(
        JetStreamContextManager contextManager,
        IGatewayDirectory? gatewayDirectory = null,
        string? shardSubjectPattern = null,
        RoutingMetrics? routingMetrics = null,
        IWatcherGatewayDirectory? watcherGatewayDirectory = null,
        RealtimeMetrics? realtimeMetrics = null,
        IConversationGatewayDirectory? conversationGatewayDirectory = null,
        IRealtimeMessageBus? messageBus = null,
        ILogger<JetStreamRealtimeEventPublisher>? logger = null,
        int maxShardParallelism = 4)
    {
        _contextManager = contextManager;
        _gatewayDirectory = gatewayDirectory ?? NullGatewayDirectory.Instance;
        _watcherGatewayDirectory = watcherGatewayDirectory ?? NullWatcherGatewayDirectory.Instance;
        _conversationGatewayDirectory = conversationGatewayDirectory ?? NullConversationGatewayDirectory.Instance;
        _shardSubjectPattern = string.IsNullOrWhiteSpace(shardSubjectPattern)
            ? null
            : shardSubjectPattern;
        // 分片发布有限并行度：小于 1 视为 1（顺序），避免无界并发打爆 NATS 连接。
        _maxShardParallelism = maxShardParallelism < 1 ? 1 : maxShardParallelism;
        _routingMetrics = routingMetrics;
        _realtimeMetrics = realtimeMetrics;
        _logger = logger;
        _messageBus = messageBus;
    }

    /// <summary>Perf-4：旧入口，回退到序列化路径。实际逻辑在 <see cref="PublishWithPayloadAsync"/>。</summary>
    public Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
        => PublishWithPayloadAsync(evt, null, ct);

    public async Task PublishWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payloadUtf8, CancellationToken ct = default)
    {
        // P0-8：优先使用预序列化的 UTF-8 字节直接传给 NATS，避免 UTF-16 中间 string。
        // 为空时回退到 SerializeToUtf8Bytes（仍是 UTF-8 字节，不经 string）。
        ReadOnlyMemory<byte> payload = payloadUtf8 is { Length: > 0 } bytes
            ? bytes
            : JsonSerializer.SerializeToUtf8Bytes(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);

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
            // 查询成功但用户离线：触发离线推送。
            _routingMetrics?.RecordBroadcastFallback("realtime", "user_offline");
            await TriggerPushDeliveryAsync(evt, evt.TargetUserId, ct).ConfigureAwait(false);
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

        await PublishToShardsParallelAsync(gateways, payload, evt, ct)
            .ConfigureAwait(false);

        _routingMetrics?.RecordShardPublish("realtime", "single", gateways.Count);
    }

    /// <summary>Perf-4：旧入口，回退到序列化路径。实际逻辑在 <see cref="PublishToManyWithPayloadAsync"/>。</summary>
    public Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default)
        => PublishToManyWithPayloadAsync(evt, null, ct);

    public async Task PublishToManyWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payloadUtf8, CancellationToken ct = default)
    {
        // P0-8：优先使用预序列化的 UTF-8 字节直接传给 NATS，避免 UTF-16 中间 string。
        ReadOnlyMemory<byte> payload = payloadUtf8 is { Length: > 0 } bytes
            ? bytes
            : JsonSerializer.SerializeToUtf8Bytes(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);

        // 非分片模式：广播单条消息，各 Gateway 自行遍历 TargetUserIds / 按 ExcludeUserId 过滤投递本机会话。
        if (_shardSubjectPattern is null)
        {
            _routingMetrics?.RecordBroadcastFallback("realtime", "no_pattern");
            await _contextManager
                .PublishRealtimeEventAsync(evt.EventId, payload, ct)
                .ConfigureAwait(false);
            return;
        }

        // Perf-2：会话级受众路由优先——一次查询返回该会话所有在线成员所在的 Gateway 实例集合，
        // 替代逐用户查询 N 个 Redis keys。
        // 极限-3：TargetUserIds 可能为 null（群 MarkRead 广播携带 ExcludeUserId），
        // 会话级路由不依赖 TargetUserIds，必须在 null 检查之前处理，否则会被单目标回退吞掉。
        if (evt.AudienceKind == AudienceKind.Conversation && !string.IsNullOrWhiteSpace(evt.ConversationId))
        {
            var convSw = Stopwatch.StartNew();
            var convLookup = await _conversationGatewayDirectory
                .GetConversationGatewaysAsync(evt.ConversationId, ct)
                .ConfigureAwait(false);
            convSw.Stop();
            _routingMetrics?.RecordDirectoryQuery("gateway", "conversation", convSw.Elapsed, convLookup.Gateways.Count);

            if (convLookup.Kind == GatewayLookupResultKind.Success && convLookup.Gateways.Count > 0)
            {
                await PublishToShardsParallelAsync(convLookup.Gateways, payload, evt, ct)
                    .ConfigureAwait(false);

                _routingMetrics?.RecordShardPublish("realtime", "conversation", convLookup.Gateways.Count);
                // 极限-3：TargetUserIds 可能为 null（会话级广播），fanout 输入用 Gateway 实例数近似。
                _routingMetrics?.RecordFanout(evt.TargetUserIds?.Length ?? convLookup.Gateways.Count, convLookup.Gateways.Count);
                return;
            }

            // LookupFailure 或无在线实例：若 TargetUserIds 为空（纯会话级广播，如群 MarkRead），
            // 无法回退到 per-user 路由，广播到所有活跃 shards，各 Gateway 按 ExcludeUserId 自行过滤。
            if (evt.TargetUserIds is null || evt.TargetUserIds.Length == 0)
            {
                await PublishToAllShardsAsync(payload, evt, "conversation_lookup_failure", ct)
                    .ConfigureAwait(false);
                return;
            }

            // 有 TargetUserIds：回退到 per-user 路由（下方逻辑）。
            _routingMetrics?.RecordBroadcastFallback("realtime", "conversation_fallback");
        }

        // 未携带多目标列表时回退到单目标发布路径。
        if (evt.TargetUserIds is null || evt.TargetUserIds.Length == 0)
        {
            await PublishWithPayloadAsync(evt, payloadUtf8, ct).ConfigureAwait(false);
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

        // 离线用户触发推送（群消息场景）：Gateway 列表为空表示该用户当前无在线会话。
        if (_messageBus is not null && evt.Type == RealtimeEventType.MessageReceived)
        {
            foreach (var kvp in lookup.GatewayMap)
            {
                if (kvp.Value is null || kvp.Value.Count == 0)
                {
                    await TriggerPushDeliveryAsync(evt, kvp.Key, ct).ConfigureAwait(false);
                }
            }
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

        await PublishToShardsParallelAsync(instances, payload, evt, ct)
            .ConfigureAwait(false);

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
    /// <param name="payload">已序列化的事件载荷（UTF-8 字节）。</param>
    /// <param name="evt">原始事件（用于日志/subject 计算）。</param>
    /// <param name="reason">fallback 原因，用于指标标签。</param>
    /// <param name="ct">取消令牌。</param>
    private async Task PublishToAllShardsAsync(
        ReadOnlyMemory<byte> payload,
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

        await PublishToShardsParallelAsync(shards, payload, evt, ct)
            .ConfigureAwait(false);

        _routingMetrics?.RecordShardPublish("realtime", "fallback", shards.Count);
    }

    /// <summary>
    /// 分片发布有限并行：向多个 Gateway shard 定向投递。
    /// <para>
    /// 单 shard 走顺序 await 快速路径（无并发开销）；多 shard 使用
    /// <see cref="Parallel.ForEachAsync"/> 限制并发度为 <c>_maxShardParallelism</c>，
    /// 避免无界 <c>Task.WhenAll</c> 在大扇出场景下打爆 NATS 连接。
    /// 四-2：任意 shard 发布失败即向上抛出 <see cref="AggregateException"/>，让上游 Outbox
    /// 整条重试。已成功 shard 依靠 JetStream MsgId（<c>eventId:instanceId</c>）去重，
    /// 重试时不会重复投递。
    /// </para>
    /// </summary>
    private async Task PublishToShardsParallelAsync(
        IEnumerable<string> instanceIds,
        ReadOnlyMemory<byte> payload,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        // 预先过滤空白实例并物化，便于计数与单 shard 快速路径判定。
        var shards = new List<string>();
        foreach (var id in instanceIds)
        {
            if (!string.IsNullOrWhiteSpace(id))
                shards.Add(id);
        }

        if (shards.Count == 0)
            return;

        // 单 shard 快速路径：保持顺序 await，无并发开销。
        if (shards.Count == 1)
        {
            await PublishSingleShardAsync(shards[0], payload, evt, ct).ConfigureAwait(false);
            return;
        }

        // 四-2：多 shard 有限并行——任意 shard 失败即向上抛出，让 Outbox 整条重试。
        // 已成功 shard 依靠 JetStream MsgId（eventId:instanceId）去重，重试时不会重复投递。
        var failures = new ConcurrentQueue<Exception>();
        await Parallel.ForEachAsync(
            shards,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _maxShardParallelism,
                CancellationToken = ct
            },
            async (instanceId, token) =>
            {
                try
                {
                    await PublishSingleShardAsync(instanceId, payload, evt, token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // 外部取消：向上传播，不视为分片失败。
                    throw;
                }
                catch (Exception ex)
                {
                    failures.Enqueue(ex);
                    _logger?.LogWarning(
                        "分片发布失败，将向上抛出触发整条重试。事件类型={Type}；事件编号={EventId}；shard={InstanceId}；原因={Message}",
                        evt.Type,
                        evt.EventId,
                        instanceId,
                        ex.Message);
                }
            }).ConfigureAwait(false);

        // 四-2：任意 shard 失败即抛出，让上游 Outbox 重试整条记录。
        // 已成功 shard 依靠 MsgId 去重，重试时不产生重复投递。
        if (failures.Count > 0)
        {
            throw new AggregateException(failures);
        }
    }

    private async Task PublishSingleShardAsync(
        string instanceId,
        ReadOnlyMemory<byte> payload,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        var subject = ShardedSubjectFormatter.Format(_shardSubjectPattern!, instanceId);
        // 使用复合 MsgId 避免 JetStream 跨 subject 去重吞掉分片消息。
        var msgId = evt.EventId + ":" + instanceId;
        await _contextManager
            .PublishRealtimeEventToSubjectAsync(subject, msgId, payload, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 离线推送触发：目标用户离线时构造 <see cref="PushDeliveryCommand"/> 并通过
    /// <see cref="IRealtimeMessageBus.PublishPushDeliveryAsync"/> 发布到 NATS，由 Push 投递方消费执行实际推送。
    /// <para>仅对 <see cref="RealtimeEventType.MessageReceived"/> 触发；回执/编辑/撤回等事件不推送。</para>
    /// <para>fire-and-forget：推送失败仅记录日志，不影响主消息投递流程。</para>
    /// </summary>
    private async Task TriggerPushDeliveryAsync(RealtimeEvent evt, long targetUserId, CancellationToken ct)
    {
        if (_messageBus is null)
            return;

        // 仅对聊天消息触发推送（收据/编辑/撤回等不推送）。
        if (evt.Type != RealtimeEventType.MessageReceived)
            return;

        try
        {
            var command = BuildPushCommand(evt, targetUserId);
            await _messageBus.PublishPushDeliveryAsync(command, ct).ConfigureAwait(false);
            _realtimeMetrics?.RecordPushTriggered(command.IsMention);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // 推送失败不影响主流程
            _logger?.LogWarning(ex, "Failed to publish push delivery for user {TargetUserId}", targetUserId);
        }
    }

    /// <summary>
    /// 从 <see cref="RealtimeEvent"/> 构造 <see cref="PushDeliveryCommand"/>。
    /// 反序列化 <see cref="RealtimeEvent.PayloadJson"/> 为 <see cref="RealtimeChatMessagePayload"/>
    /// 提取消息正文与 @mention 信息；解析失败回退到默认文案。
    /// </summary>
    private static PushDeliveryCommand BuildPushCommand(RealtimeEvent evt, long targetUserId)
    {
        const string defaultTitle = "New Message";
        const string defaultBody = "You have a new message";
        string title = defaultTitle;
        string body = defaultBody;
        bool isMention = false;

        if (!string.IsNullOrEmpty(evt.PayloadJson))
        {
            try
            {
                var payload = JsonSerializer.Deserialize(
                    evt.PayloadJson, RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
                if (payload is not null)
                {
                    body = payload.Content;
                    // Check if target user is mentioned
                    if (payload.MentionedUserIds is not null && payload.MentionedUserIds.Count > 0)
                    {
                        isMention = payload.MentionedUserIds.Contains(targetUserId);
                    }
                }
            }
            catch (JsonException)
            {
                // Fall back to defaults
            }
        }

        return new PushDeliveryCommand
        {
            TargetUserId = targetUserId,
            Title = title,
            Body = body,
            ConversationId = evt.ConversationId,
            MessageId = evt.MessageId,
            SenderDisplayName = null,
            IsMention = isMention,
            OccurredAtMs = evt.OccurredAtMs
        };
    }
}

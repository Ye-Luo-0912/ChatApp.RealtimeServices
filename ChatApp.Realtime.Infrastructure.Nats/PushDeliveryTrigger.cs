using System.Text.Json;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats;

/// <summary>
/// 离线推送触发器：从 <see cref="RealtimeEvent"/> 构造 <see cref="PushDeliveryCommand"/>
/// 并通过 <see cref="IRealtimeMessageBus.PublishPushDeliveryAsync"/> 发布到 NATS。
/// <para>
/// 供 <see cref="JetStream.JetStreamRealtimeEventPublisher"/> 与
/// <see cref="Queueing.NatsRealtimeEventPublisher"/> 共享，消除两份重复的
/// TriggerPushDeliveryAsync / BuildPushCommand 实现（DRY）。
/// </para>
/// <para>
/// 仅对 <see cref="RealtimeEventType.MessageReceived"/> 触发；回执/编辑/撤回等事件不推送。
/// fire-and-forget：推送失败仅记录日志，不影响主消息投递流程。
/// </para>
/// </summary>
internal sealed class PushDeliveryTrigger
{
    private readonly IRealtimeMessageBus? _messageBus;
    private readonly RealtimeMetrics? _realtimeMetrics;
    private readonly ILogger? _logger;

    public PushDeliveryTrigger(
        IRealtimeMessageBus? messageBus,
        RealtimeMetrics? realtimeMetrics,
        ILogger? logger)
    {
        _messageBus = messageBus;
        _realtimeMetrics = realtimeMetrics;
        _logger = logger;
    }

    /// <summary>
    /// MessageBus 未注入时此触发器不生效（测试 / 未配置 Push 的部署场景）。
    /// </summary>
    public bool IsEnabled => _messageBus is not null;

    /// <summary>
    /// 触发离线推送。仅对 <see cref="RealtimeEventType.MessageReceived"/> 生效；
    /// 其他事件类型静默跳过。推送失败不影响主流程。
    /// </summary>
    public async Task TriggerAsync(RealtimeEvent evt, long targetUserId, CancellationToken ct)
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
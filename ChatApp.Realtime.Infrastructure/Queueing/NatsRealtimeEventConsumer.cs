using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Queueing;

/// <summary>
/// 基于 Core NATS 的实时事件消费者。
/// 当前用于验证事件流闭环；需要事件重放、确认和持久订阅时再切换为 JetStream consumer。
/// </summary>
public sealed class NatsRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsRealtimeEventConsumer> _logger;

    public NatsRealtimeEventConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    /// <summary>
    /// 从NATS订阅主题中异步消费实时事件。
    /// </summary>
    /// <param name="ct">用于取消操作的CancellationToken。</param>
    /// <returns>返回一个异步枚举，该枚递归地提供<see cref="RealtimeEvent"/>对象。</returns>
    /// <remarks>
    /// 本方法订阅了配置指定的主题和队列组，并将接收到的消息反序列化为<see cref="RealtimeEvent"/>实例。
    /// 如果消息为空或反序列化失败，则会记录警告或错误日志并跳过该消息。
    /// 每个成功处理的消息都会被作为<see cref="RealtimeEvent"/>实例返回。
    /// </remarks>
    public async IAsyncEnumerable<RealtimeEvent> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 实时事件消费者已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.RealtimeEvents,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.RealtimeEvents,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            RealtimeEvent? evt = null;

            try
            {
                msg.EnsureSuccess();

                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("NATS 实时事件为空，已跳过。Subject={Subject}", msg.Subject);
                    continue;
                }

                evt = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.RealtimeEvent);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "NATS 实时事件反序列化失败。Subject={Subject}", msg.Subject);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NATS 实时事件读取失败。Subject={Subject}", msg.Subject);
            }

            if (evt is not null)
            {
                yield return evt;
            }
        }
    }
}

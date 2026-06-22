using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Queueing;

/// <summary>
/// 基于 Core NATS 的实时事件发布器。
/// 发布内容保持为显式 JSON 字符串，避免依赖客户端默认 JSON 反射序列化策略。
/// </summary>
public sealed class NatsRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsRealtimeEventPublisher> _logger;

    public NatsRealtimeEventPublisher(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsRealtimeEventPublisher> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    /// <summary>
    /// 将实时事件异步发布到NATS消息队列。
    /// 该方法接受一个<see cref="RealtimeEvent"/>实例作为参数，将其序列化为JSON字符串后通过指定的主题发布出去。
    /// </summary>
    /// <param name="evt">要发布的实时事件。</param>
    /// <param name="ct">用于取消操作的<see cref="CancellationToken"/>。默认值为<see cref="CancellationToken.None"/>。</param>
    /// <returns>表示任务完成状态的任务对象。</returns>
    /// <remarks>
    /// 发布成功后，将在日志中记录一条信息级别日志，包含事件编号、类型、目标用户ID以及使用的主题名称。
    /// 如果在执行过程中遇到任何问题（如网络中断或NATS服务不可用），异常将被抛出，并且可能需要调用者进行适当的错误处理。
    /// </remarks>
    public async Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(
            evt,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);

        await _connectionClient.Client
            .PublishAsync(_options.Topics.RealtimeEvents, json, cancellationToken: ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "实时事件已发布到 NATS。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}；Subject={Subject}",
            evt.EventId,
            evt.Type,
            evt.TargetUserId,
            _options.Topics.RealtimeEvents);
    }
}

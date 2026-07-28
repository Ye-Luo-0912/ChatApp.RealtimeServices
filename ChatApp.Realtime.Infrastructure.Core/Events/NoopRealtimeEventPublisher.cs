using ChatApp.Realtime.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Events;

public sealed class NoopRealtimeEventPublisher : IRealtimeEventPublisher
{
    private readonly ILogger<NoopRealtimeEventPublisher> _logger;

    public NoopRealtimeEventPublisher(ILogger<NoopRealtimeEventPublisher> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "P0 默认实现跳过实时事件发布。事件编号={EventId}；类型={Type}；目标用户={TargetUserId}",
            evt.EventId,
            evt.Type,
            evt.TargetUserId);

        return Task.CompletedTask;
    }

    public Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default) => Task.CompletedTask;

    /// <summary>Perf-4：Noop 实现直接复用 <see cref="PublishAsync"/>，忽略预序列化 payload。</summary>
    public Task PublishWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payload, CancellationToken ct = default)
        => PublishAsync(evt, ct);

    /// <summary>Perf-4：Noop 实现直接复用 <see cref="PublishToManyAsync"/>，忽略预序列化 payload。</summary>
    public Task PublishToManyWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payload, CancellationToken ct = default)
        => PublishToManyAsync(evt, ct);
}

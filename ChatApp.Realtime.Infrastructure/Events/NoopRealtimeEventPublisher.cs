using ChatApp.Realtime.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Events;

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
}

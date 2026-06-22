using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Events;

public sealed class NoopRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly ILogger<NoopRealtimeEventConsumer> _logger;

    public NoopRealtimeEventConsumer(ILogger<NoopRealtimeEventConsumer> logger)
    {
        _logger = logger;
    }

    public async IAsyncEnumerable<RealtimeEvent> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("尚未配置真实实时事件消费者。");
        await Task.CompletedTask;
        yield break;
    }
}

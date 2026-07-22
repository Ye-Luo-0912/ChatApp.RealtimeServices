using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Events;

public sealed class NoopRealtimeEventConsumer(ILogger<NoopRealtimeEventConsumer> logger) : IRealtimeEventConsumer
{
    public async IAsyncEnumerable<RealtimeEventEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogDebug("尚未配置真实实时事件消费者。");
        await Task.CompletedTask;
        yield break;
    }
}

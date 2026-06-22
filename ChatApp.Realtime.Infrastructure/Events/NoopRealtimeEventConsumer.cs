using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Events;

public sealed class NoopRealtimeEventConsumer : IRealtimeEventConsumer
{
    private readonly ILogger<NoopRealtimeEventConsumer> _logger;

    public NoopRealtimeEventConsumer(ILogger<NoopRealtimeEventConsumer> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 异步消费实时事件。此方法在当前实现中为占位符，实际不执行任何操作。
    /// </summary>
    /// <param name="ct">用于请求取消异步操作的令牌。</param>
    /// <returns>返回一个空的异步枚举，表示没有事件被消费。</returns>
    public async IAsyncEnumerable<RealtimeEvent> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("尚未配置真实实时事件消费者。");
        await Task.CompletedTask;
        yield break;
    }
}

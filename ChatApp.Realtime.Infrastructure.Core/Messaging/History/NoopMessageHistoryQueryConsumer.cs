using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Messaging.History;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging.History;

public sealed class NoopMessageHistoryQueryConsumer : IMessageHistoryQueryConsumer
{
    private readonly ILogger<NoopMessageHistoryQueryConsumer> _logger;

    public NoopMessageHistoryQueryConsumer(
        ILogger<NoopMessageHistoryQueryConsumer> logger)
    {
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageHistoryQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _logger.LogDebug("尚未配置历史消息查询消费者。");
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

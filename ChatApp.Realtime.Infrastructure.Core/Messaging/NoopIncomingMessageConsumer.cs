using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Messaging;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class NoopIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly ILogger<NoopIncomingMessageConsumer> _logger;

    public NoopIncomingMessageConsumer(ILogger<NoopIncomingMessageConsumer> logger)
    {
        _logger = logger;
    }

    public async IAsyncEnumerable<IncomingMessageEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("尚未配置真实入站消息消费者。");
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

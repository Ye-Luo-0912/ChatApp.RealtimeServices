using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Messaging;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class NoopMessageReceiptConsumer : IMessageReceiptConsumer
{
    private readonly ILogger<NoopMessageReceiptConsumer> _logger;

    public NoopMessageReceiptConsumer(ILogger<NoopMessageReceiptConsumer> logger)
    {
        _logger = logger;
    }

    public async IAsyncEnumerable<MessageReceiptEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _logger.LogDebug("尚未配置真实消息回执消费者。");
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
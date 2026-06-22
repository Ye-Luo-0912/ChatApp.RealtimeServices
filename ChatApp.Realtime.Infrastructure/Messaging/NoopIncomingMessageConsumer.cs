using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Messaging;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Messaging;

public sealed class NoopIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly ILogger<NoopIncomingMessageConsumer> _logger;

    public NoopIncomingMessageConsumer(ILogger<NoopIncomingMessageConsumer> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 消费入站消息的异步方法。此方法在没有配置真实的消息消费者时，仅用于调试或占位用途。
    /// </summary>
    /// <param name="ct">取消令牌，用于支持操作的取消。</param>
    /// <returns>返回一个可异步枚举的序列，表示接收到的入站消息命令。在这个实现中，由于是空操作消费者，因此不会实际返回任何值。</returns>
    /// <remarks>
    /// 该方法会记录一条日志信息，指出尚未配置真实的入站消息消费者，并且在完成任务后立即退出（yield break）。
    /// </remarks>
    public async IAsyncEnumerable<IncomingMessageCommand> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogDebug("尚未配置真实入站消息消费者。");
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

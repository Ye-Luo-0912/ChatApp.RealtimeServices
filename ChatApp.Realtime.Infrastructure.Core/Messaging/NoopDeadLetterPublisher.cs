using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class NoopDeadLetterPublisher : IDeadLetterPublisher
{
    public Task PublishAsync(DeadLetterMessage message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置可靠的死信发布器，拒绝丢弃失败消息。");
    }
}

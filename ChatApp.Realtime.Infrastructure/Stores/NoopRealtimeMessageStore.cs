using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Stores;

public sealed class NoopRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly ILogger<NoopRealtimeMessageStore> _logger;

    public NoopRealtimeMessageStore(ILogger<NoopRealtimeMessageStore> logger)
    {
        _logger = logger;
    }

    public Task SaveAsync(RealtimeMessageRecord message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "P0 默认实现跳过实时消息入库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        return Task.CompletedTask;
    }
}

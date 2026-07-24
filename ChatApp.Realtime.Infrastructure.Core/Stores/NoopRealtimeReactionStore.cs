using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeReactionStore(ILogger<NoopRealtimeReactionStore> logger)
    : IRealtimeReactionStore
{
    public Task<MessageReactionPersistResult> AddAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        MessageReactionOptions options,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogCritical("未配置反应存储，拒绝添加反应。消息={MessageId}", messageId);
        throw new InvalidOperationException("未配置真实反应存储。");
    }

    public Task<MessageReactionPersistResult> RemoveAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogCritical("未配置反应存储，拒绝移除反应。消息={MessageId}", messageId);
        throw new InvalidOperationException("未配置真实反应存储。");
    }

    public Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<MessageReactionRecord>>([]);
    }
}

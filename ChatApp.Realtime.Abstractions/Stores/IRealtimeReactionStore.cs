using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeReactionStore
{
    Task<MessageReactionPersistResult> AddAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        MessageReactionOptions options,
        CancellationToken ct = default);

    Task<MessageReactionPersistResult> RemoveAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        CancellationToken ct = default);

    Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default);

    /// <summary>
    /// 六-4：账号清理时删除该用户的全部反应记录，返回已删除行数。
    /// </summary>
    Task<int> DeleteByUserAsync(long userId, CancellationToken ct = default);
}

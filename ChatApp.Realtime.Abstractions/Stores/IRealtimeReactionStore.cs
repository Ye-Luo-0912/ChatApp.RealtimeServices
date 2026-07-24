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
}

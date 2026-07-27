using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Tests.TestDoubles;

/// <summary>
/// LongTerm-1：测试用 Noop tombstone / 幂等账本。无 logger 依赖，保持测试简洁。
/// 默认行为：IsUserDeleted=false、FindAsync=null、Record/Purge 为空操作。
/// </summary>
internal static class NoopTombstoneAndLedger
{
    public static IUserDeletionTombstoneStore Tombstone { get; } = new NoopTombstoneStore();
    public static ICommandIdempotencyLedger Ledger { get; } = new NoopLedger();

    private sealed class NoopTombstoneStore : IUserDeletionTombstoneStore
    {
        public Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(false);

        public Task<UserLifecycleState> GetLifecycleStateAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(UserLifecycleState.Active);

        public Task RecordDeletionAsync(
            long userId,
            string deletionEventId,
            long deletedAtMs,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }

    private sealed class NoopLedger : ICommandIdempotencyLedger
    {
        public Task<IdempotencyLedgerEntry?> FindAsync(
            long senderUserId,
            string clientMessageId,
            CancellationToken ct = default) =>
            Task.FromResult<IdempotencyLedgerEntry?>(null);

        public Task RecordAsync(
            string commandId,
            long senderUserId,
            string clientMessageId,
            string contentFingerprint,
            IdempotencyLedgerResultKind kind,
            string? messageId,
            long receivedAtMs,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }
}

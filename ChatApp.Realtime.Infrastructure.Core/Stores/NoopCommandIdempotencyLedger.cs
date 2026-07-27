using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// LongTerm-1：Noop 幂等账本。测试与未配置 PostgreSQL 时使用。
/// FindAsync 始终返回 null（未处理），RecordAsync / PurgeOlderThanAsync 为空操作。
/// 此时幂等性回退到 messages 表唯一索引（原有行为）。
/// <para>
/// P0-3：RecordAsync 签名不变（仍为 Task），Npgsql 实现内部已改为
/// ON CONFLICT DO NOTHING 以保护 canonical 记录不被并发请求覆盖。
/// </para>
/// </summary>
public sealed class NoopCommandIdempotencyLedger(ILogger<NoopCommandIdempotencyLedger> logger)
    : ICommandIdempotencyLedger
{
    public Task<IdempotencyLedgerEntry?> FindAsync(
        long senderUserId,
        string clientMessageId,
        CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop ledger find: sender={SenderUserId}; clientMessageId={ClientMessageId} → miss",
            senderUserId,
            clientMessageId);
        return Task.FromResult<IdempotencyLedgerEntry?>(null);
    }

    public Task RecordAsync(
        string commandId,
        long senderUserId,
        string clientMessageId,
        string contentFingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop ledger record skipped: sender={SenderUserId}; clientMessageId={ClientMessageId}; kind={Kind}",
            senderUserId,
            clientMessageId,
            kind);
        return Task.CompletedTask;
    }

    public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default)
    {
        logger.LogDebug("Noop ledger purge skipped: cutoff={Cutoff}", cutoffMs);
        return Task.FromResult(0L);
    }
}

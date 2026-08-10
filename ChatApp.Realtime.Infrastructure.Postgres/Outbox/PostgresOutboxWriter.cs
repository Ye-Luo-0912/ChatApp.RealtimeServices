using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;

namespace ChatApp.Realtime.Infrastructure.Postgres.Outbox;

/// <summary>
/// Outbox 写入：将业务事件以 <c>ON CONFLICT (event_id) DO NOTHING</c> 的幂等方式
/// 写入当前事务的 Outbox 表，支持单条与批量（聚合事件携带 <c>TargetUserIds</c>）。
/// </summary>
/// <remarks>
/// Perf-9：SQL 实现下沉到 <see cref="OutboxInsertHelper"/>，与 Reaction/Conversation Store 共享同一份
/// UNNEST INSERT，消除三份重复实现。本类仅保留 <see cref="RealtimeWriteSession"/> 适配层。
/// </remarks>
internal sealed class PostgresOutboxWriter
{
    private readonly RealtimeWriteSession _session;

    public PostgresOutboxWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    public async Task<int> InsertAsync(RealtimeEvent evt)
    {
        RealtimeOutboxPreclaim? preclaim = null;
        // 当前高频单聊事件使用原生 target_user_ids 单行 INSERT；只有该路径预领取，
        // 批量/冲突路径继续使用原精确认领，避免无法从聚合 row-count 反推逐行结果。
        if (evt.TargetUserIds is { Length: > 0 }
            && _session.TryReserveOutboxPreclaim(evt.EventId, out var reserved))
        {
            preclaim = reserved;
        }

        var inserted = await OutboxInsertHelper.InsertAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            evt,
            _session.CancellationToken,
            preclaim).ConfigureAwait(false);
        // Reliability-4：累计到 session，由 CommitAsync 在事务提交成功后统一记录到 metrics。
        _session.RecordOutboxInsert(inserted, evt, preclaim);
        return inserted;
    }

    public async Task<int> InsertManyAsync(IReadOnlyList<RealtimeEvent> events)
    {
        var inserted = await OutboxInsertHelper.InsertManyAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            events,
            _session.CancellationToken).ConfigureAwait(false);
        _session.RecordOutboxInserts(inserted, events);
        return inserted;
    }
}

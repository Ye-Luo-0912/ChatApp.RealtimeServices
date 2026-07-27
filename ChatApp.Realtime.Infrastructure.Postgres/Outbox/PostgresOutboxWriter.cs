using ChatApp.Realtime.Abstractions.Events;
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
        var inserted = await OutboxInsertHelper.InsertAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            evt,
            _session.CancellationToken).ConfigureAwait(false);
        // Reliability-4：累计到 session，由 CommitAsync 在事务提交成功后统一记录到 metrics。
        _session.RecordOutboxInsert(inserted);
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
        _session.RecordOutboxInsert(inserted);
        return inserted;
    }
}

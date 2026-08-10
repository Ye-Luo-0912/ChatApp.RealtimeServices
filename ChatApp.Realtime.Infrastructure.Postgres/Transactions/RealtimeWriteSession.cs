using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Transactions;

/// <summary>
/// 共享事务上下文：在一次业务变更（如写入一条实时消息）中，保证消息写入、附件绑定、
/// 会话投影、未读数、Outbox 等组件共享同一个 <see cref="NpgsqlConnection"/> /
/// <see cref="NpgsqlTransaction"/> / <see cref="CancellationToken"/>，不增加数据库往返，
/// 也不需要在每个 Writer 里重复打开连接。
/// </summary>
internal sealed class RealtimeWriteSession : IAsyncDisposable
{
    private readonly RealtimeMetrics? _metrics;
    private readonly IRealtimeOutboxSignal? _outboxSignal;
    private readonly IRealtimeOutboxPreclaimCoordinator? _preclaimCoordinator;
    // Reliability-4：累计本事务内 Outbox 实际插入行数（已扣除 ON CONFLICT DO NOTHING 跳过的重复行）。
    // 仅在 CommitAsync 成功后调用 RecordOutboxEnqueued，回滚时丢弃，避免 realtime.outbox.pending 漂移。
    private int _pendingOutboxInserts;
    private string? _singleCommittedEventId;
    private List<string>? _committedEventIds;
    private RealtimeOutboxPreclaim? _singlePreclaim;
    private List<RealtimeOutboxPreclaim>? _preclaims;

    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    public RealtimeDatabaseSchema Schema { get; }
    public CancellationToken CancellationToken { get; }

    internal RealtimeWriteSession(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken,
        RealtimeMetrics? metrics = null,
        IRealtimeOutboxSignal? outboxSignal = null)
    {
        Connection = connection;
        Transaction = transaction;
        Schema = schema;
        CancellationToken = cancellationToken;
        _metrics = metrics;
        _outboxSignal = outboxSignal;
        _preclaimCoordinator = outboxSignal as IRealtimeOutboxPreclaimCoordinator;
    }

    internal bool TryReserveOutboxPreclaim(
        string eventId,
        out RealtimeOutboxPreclaim preclaim)
    {
        preclaim = default;
        if (_preclaimCoordinator is null
            || !_preclaimCoordinator.TryReservePreclaim(eventId, out preclaim))
        {
            return false;
        }

        if (_preclaims is not null)
        {
            _preclaims.Add(preclaim);
        }
        else if (_singlePreclaim is null)
        {
            _singlePreclaim = preclaim;
        }
        else
        {
            _preclaims ??= new List<RealtimeOutboxPreclaim>(4) { _singlePreclaim.Value };
            _preclaims.Add(preclaim);
        }

        return true;
    }

    /// <summary>
    /// Reliability-4：由 Outbox Writer 在 INSERT 成功后调用，累计实际插入行数。
    /// 仅在事务提交后才会反映到 <see cref="RealtimeMetrics"/> 的 pending 指标。
    /// </summary>
    internal void RecordOutboxInsert(
        int insertedCount,
        RealtimeEvent evt,
        RealtimeOutboxPreclaim? preclaim = null)
    {
        if (insertedCount > 0)
        {
            Interlocked.Add(ref _pendingOutboxInserts, insertedCount);
            if (preclaim is null)
                RecordCommittedEventId(evt.EventId);
        }
        else if (preclaim is not null)
        {
            CancelPreclaim(preclaim.Value);
        }
    }

    internal void RecordOutboxInserts(int insertedCount, IReadOnlyList<RealtimeEvent> events)
    {
        if (insertedCount <= 0)
            return;

        Interlocked.Add(ref _pendingOutboxInserts, insertedCount);
        // ON CONFLICT 的极少数部分命中无法从 ExecuteNonQuery 得到逐行编号；多提示是安全的，
        // 精确认领会忽略不存在/已完成的 event_id。
        for (var i = 0; i < events.Count; i++)
            RecordCommittedEventId(events[i].EventId);
    }

    public async Task CommitAsync()
    {
        await Transaction.CommitAsync(CancellationToken).ConfigureAwait(false);
        // Reliability-4：事务提交成功后再记录入队指标，避免回滚后 gauge 向上漂移。
        var pending = Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        if (pending > 0)
            _metrics?.RecordOutboxEnqueued(pending);
        CommitPreclaims();
        NotifyCommittedEventIds();
    }

    public Task RollbackAsync()
    {
        // 回滚时丢弃累计的入队计数，不记录到 metrics。
        Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        CancelPreclaims();
        ClearCommittedEventIds();
        return Transaction.RollbackAsync(CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose 时如果有未提交的入队计数，直接丢弃。
        Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        CancelPreclaims();
        ClearCommittedEventIds();
        await Transaction.DisposeAsync().ConfigureAwait(false);
        await Connection.DisposeAsync().ConfigureAwait(false);
    }

    private void RecordCommittedEventId(string eventId)
    {
        if (_singleCommittedEventId is null)
        {
            _singleCommittedEventId = eventId;
            return;
        }

        _committedEventIds ??= new List<string>(4) { _singleCommittedEventId };
        _committedEventIds.Add(eventId);
    }

    private void NotifyCommittedEventIds()
    {
        try
        {
            if (_outboxSignal is null)
                return;

            if (_committedEventIds is null)
            {
                if (_singleCommittedEventId is not null)
                    TryNotify(_singleCommittedEventId);
            }
            else
            {
                for (var i = 0; i < _committedEventIds.Count; i++)
                    TryNotify(_committedEventIds[i]);
            }
        }
        finally
        {
            ClearCommittedEventIds();
        }
    }

    private void CommitPreclaims()
    {
        try
        {
            if (_preclaimCoordinator is null)
                return;

            if (_preclaims is null)
            {
                if (_singlePreclaim is { } single)
                    TryCommitPreclaim(single);
            }
            else
            {
                for (var i = 0; i < _preclaims.Count; i++)
                    TryCommitPreclaim(_preclaims[i]);
            }
        }
        finally
        {
            ClearPreclaims();
        }
    }

    private void TryCommitPreclaim(RealtimeOutboxPreclaim preclaim)
    {
        try
        {
            _preclaimCoordinator!.CommitPreclaim(preclaim);
        }
        catch
        {
            // 事务已经提交，提示失败不能伪报业务失败。租约到期后恢复扫描会接管。
            try
            {
                _preclaimCoordinator!.CancelPreclaim(preclaim);
            }
            catch
            {
                // 自定义协调器的清理失败同样不能越过事务提交边界。
            }
        }
    }

    private void CancelPreclaim(RealtimeOutboxPreclaim preclaim)
    {
        if (_preclaimCoordinator is null)
            return;

        _preclaimCoordinator.CancelPreclaim(preclaim);
        if (_preclaims is not null)
        {
            _preclaims.Remove(preclaim);
            if (_preclaims.Count == 1)
            {
                _singlePreclaim = _preclaims[0];
                _preclaims.Clear();
                _preclaims = null;
            }
        }
        else if (_singlePreclaim == preclaim)
        {
            _singlePreclaim = null;
        }
    }

    private void CancelPreclaims()
    {
        if (_preclaimCoordinator is not null)
        {
            if (_preclaims is null)
            {
                if (_singlePreclaim is { } single)
                    _preclaimCoordinator.CancelPreclaim(single);
            }
            else
            {
                for (var i = 0; i < _preclaims.Count; i++)
                    _preclaimCoordinator.CancelPreclaim(_preclaims[i]);
            }
        }

        ClearPreclaims();
    }

    private void ClearPreclaims()
    {
        _singlePreclaim = null;
        _preclaims?.Clear();
    }

    private void TryNotify(string eventId)
    {
        try
        {
            _outboxSignal!.Notify(eventId);
        }
        catch
        {
            // 提示是非权威加速通道。事务已经提交，绝不能因为自定义信号实现异常而向
            // 上游伪报写入失败；发布器的周期恢复扫描会重新发现这条 Pending 记录。
        }
    }

    private void ClearCommittedEventIds()
    {
        _singleCommittedEventId = null;
        _committedEventIds?.Clear();
    }
}

/// <summary>
/// 打开连接并开启事务，封装为 <see cref="RealtimeWriteSession"/>。
/// 由 <see cref="NpgsqlRealtimeMessageStore"/> 在每个公共方法入口调用。
/// </summary>
internal sealed class RealtimeWriteSessionFactory
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _schema;
    private readonly RealtimeMetrics? _metrics;
    private readonly IRealtimeOutboxSignal? _outboxSignal;

    public RealtimeWriteSessionFactory(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema,
        RealtimeMetrics? metrics = null,
        IRealtimeOutboxSignal? outboxSignal = null)
    {
        _databaseClient = databaseClient;
        _schema = schema;
        _metrics = metrics;
        _outboxSignal = outboxSignal;
    }

    public async Task<RealtimeWriteSession> BeginAsync(CancellationToken cancellationToken)
    {
        var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);
            return new RealtimeWriteSession(
                connection,
                transaction,
                _schema,
                cancellationToken,
                _metrics,
                _outboxSignal);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

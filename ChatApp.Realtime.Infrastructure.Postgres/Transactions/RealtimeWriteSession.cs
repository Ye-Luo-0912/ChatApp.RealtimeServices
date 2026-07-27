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
    // Reliability-4：累计本事务内 Outbox 实际插入行数（已扣除 ON CONFLICT DO NOTHING 跳过的重复行）。
    // 仅在 CommitAsync 成功后调用 RecordOutboxEnqueued，回滚时丢弃，避免 realtime.outbox.pending 漂移。
    private int _pendingOutboxInserts;

    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    public RealtimeDatabaseSchema Schema { get; }
    public CancellationToken CancellationToken { get; }

    internal RealtimeWriteSession(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken,
        RealtimeMetrics? metrics = null)
    {
        Connection = connection;
        Transaction = transaction;
        Schema = schema;
        CancellationToken = cancellationToken;
        _metrics = metrics;
    }

    /// <summary>
    /// Reliability-4：由 Outbox Writer 在 INSERT 成功后调用，累计实际插入行数。
    /// 仅在事务提交后才会反映到 <see cref="RealtimeMetrics"/> 的 pending 指标。
    /// </summary>
    internal void RecordOutboxInsert(int insertedCount)
    {
        if (insertedCount > 0)
            Interlocked.Add(ref _pendingOutboxInserts, insertedCount);
    }

    public async Task CommitAsync()
    {
        await Transaction.CommitAsync(CancellationToken).ConfigureAwait(false);
        // Reliability-4：事务提交成功后再记录入队指标，避免回滚后 gauge 向上漂移。
        var pending = Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        if (pending > 0)
            _metrics?.RecordOutboxEnqueued(pending);
    }

    public Task RollbackAsync()
    {
        // 回滚时丢弃累计的入队计数，不记录到 metrics。
        Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        return Transaction.RollbackAsync(CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose 时如果有未提交的入队计数，直接丢弃。
        Interlocked.Exchange(ref _pendingOutboxInserts, 0);
        await Transaction.DisposeAsync().ConfigureAwait(false);
        await Connection.DisposeAsync().ConfigureAwait(false);
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

    public RealtimeWriteSessionFactory(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema,
        RealtimeMetrics? metrics = null)
    {
        _databaseClient = databaseClient;
        _schema = schema;
        _metrics = metrics;
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
            return new RealtimeWriteSession(connection, transaction, _schema, cancellationToken, _metrics);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

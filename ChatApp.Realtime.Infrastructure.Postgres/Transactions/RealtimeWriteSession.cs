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
    public NpgsqlConnection Connection { get; }
    public NpgsqlTransaction Transaction { get; }
    public RealtimeDatabaseSchema Schema { get; }
    public CancellationToken CancellationToken { get; }

    internal RealtimeWriteSession(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        Connection = connection;
        Transaction = transaction;
        Schema = schema;
        CancellationToken = cancellationToken;
    }

    public Task CommitAsync() => Transaction.CommitAsync(CancellationToken);

    public Task RollbackAsync() => Transaction.RollbackAsync(CancellationToken);

    public async ValueTask DisposeAsync()
    {
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

    public RealtimeWriteSessionFactory(RealtimeDatabaseClient databaseClient, RealtimeDatabaseSchema schema)
    {
        _databaseClient = databaseClient;
        _schema = schema;
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
            return new RealtimeWriteSession(connection, transaction, _schema, cancellationToken);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

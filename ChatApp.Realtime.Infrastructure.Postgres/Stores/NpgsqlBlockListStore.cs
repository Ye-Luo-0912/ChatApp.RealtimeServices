using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 二-2：基于 PostgreSQL 的屏蔽关系查询实现。
/// <para>
/// 查询 <c>public."T_BlockRecords"</c> 表，确认 <c>senderUserId</c> 是否被 <c>receiverUserId</c> 屏蔽。
/// </para>
/// <para>
/// 查询故障时抛出异常，由调用方按 fail-closed 策略处理（拒绝写入）。
/// </para>
/// </summary>
public sealed class NpgsqlBlockListStore : IBlockListStore
{
    private readonly RealtimeDatabaseClient _databaseClient;

    public NpgsqlBlockListStore(RealtimeDatabaseClient databaseClient)
    {
        _databaseClient = databaseClient;
    }

    public async Task<bool> IsBlockedAsync(
        long receiverUserId,
        long senderUserId,
        CancellationToken cancellationToken = default)
    {
        if (receiverUserId <= 0 || senderUserId <= 0)
            return false;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            """SELECT 1 FROM public."T_BlockRecords" WHERE "BlockerId" = @receiver_id AND "BlockedUserId" = @sender_id LIMIT 1;""",
            connection);
        command.Parameters.AddWithValue("receiver_id", receiverUserId);
        command.Parameters.AddWithValue("sender_id", senderUserId);

        var result = await command.ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);
        return result is not null;
    }
}

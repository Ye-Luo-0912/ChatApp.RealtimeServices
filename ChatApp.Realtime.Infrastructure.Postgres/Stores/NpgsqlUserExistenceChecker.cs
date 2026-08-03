using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 二-1：基于 PostgreSQL 的用户存在性校验实现。
/// <para>
/// 查询 <c>public."AspNetUsers"</c> 表（与应用层共享数据库），按 Id 确认用户是否存在。
/// </para>
/// <para>
/// 查询故障时抛出异常，由调用方按 fail-closed 策略处理（拒绝写入）。
/// </para>
/// </summary>
public sealed class NpgsqlUserExistenceChecker : IUserExistenceChecker
{
    private readonly RealtimeDatabaseClient _databaseClient;

    public NpgsqlUserExistenceChecker(RealtimeDatabaseClient databaseClient)
    {
        _databaseClient = databaseClient;
    }

    public async Task<bool> ExistsAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0)
            return false;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            """SELECT 1 FROM public."AspNetUsers" WHERE "Id" = @user_id LIMIT 1;""",
            connection);
        command.Parameters.AddWithValue("user_id", userId);

        var result = await command.ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);
        return result is not null;
    }

    public async Task<IReadOnlyList<long>> FilterNonExistentAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        if (userIds is null || userIds.Count == 0)
            return Array.Empty<long>();

        var candidateIds = userIds.Where(id => id > 0).Distinct().ToArray();
        if (candidateIds.Length == 0)
            return Array.Empty<long>();

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            """SELECT "Id" FROM public."AspNetUsers" WHERE "Id" = ANY(@user_ids);""",
            connection);
        command.Parameters.AddWithValue("user_ids", candidateIds);

        var existing = new HashSet<long>(candidateIds.Length);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            existing.Add(reader.GetInt64(0));

        return candidateIds.Where(id => !existing.Contains(id)).ToArray();
    }
}

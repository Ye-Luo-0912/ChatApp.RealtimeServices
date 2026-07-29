using System.Data.Common;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL membership periods 存储。
/// <para>
/// 事务内写入方法复用调用方已有的连接与事务（与群操作同生共死），
/// <see cref="GetMembershipPeriodsAsync"/> 使用独立连接读取。
/// </para>
/// </summary>
public sealed class NpgsqlMembershipPeriodStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema) : IMembershipPeriodStore
{
    /// <summary>
    /// 在业务事务内记录入群。使用 ON CONFLICT DO NOTHING 确保幂等。
    /// </summary>
    public async Task RecordJoinInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long joinedAtMs,
        CancellationToken ct = default)
    {
        var npgsqlConnection = (NpgsqlConnection)connection;
        var npgsqlTransaction = (NpgsqlTransaction)transaction;

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.MembershipPeriodsTableSql}
                 (conversation_id, user_id, joined_at_ms, left_at_ms, left_reason)
             VALUES
                 (@conversation_id, @user_id, @joined_at_ms, NULL, NULL)
             ON CONFLICT (conversation_id, user_id, joined_at_ms) DO NOTHING;
             """,
            npgsqlConnection,
            npgsqlTransaction);

        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 在业务事务内批量记录入群。使用 UNNEST 单条 SQL 写入所有用户，
    /// 避免逐成员往返；ON CONFLICT DO NOTHING 保证幂等。
    /// </summary>
    public async Task RecordJoinsBatchInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long joinedAtMs,
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        if (userIds.Count == 0)
            return;

        var npgsqlConnection = (NpgsqlConnection)connection;
        var npgsqlTransaction = (NpgsqlTransaction)transaction;

        var userIdArray = userIds as long[] ?? userIds.ToArray();
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.MembershipPeriodsTableSql}
                 (conversation_id, user_id, joined_at_ms, left_at_ms, left_reason)
             SELECT @conversation_id, t.user_id, @joined_at_ms, NULL, NULL
             FROM UNNEST(@user_ids) AS t(user_id)
             ON CONFLICT (conversation_id, user_id, joined_at_ms) DO NOTHING;
             """,
            npgsqlConnection,
            npgsqlTransaction);

        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        command.Parameters.Add(new NpgsqlParameter("user_ids", NpgsqlDbType.Bigint | NpgsqlDbType.Array)
        {
            Value = userIdArray
        });

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 在业务事务内记录离群。仅更新 <c>left_at_ms IS NULL</c> 的当前活跃时间段。
    /// </summary>
    public async Task RecordLeaveInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long leftAtMs,
        string leftReason,
        CancellationToken ct = default)
    {
        var npgsqlConnection = (NpgsqlConnection)connection;
        var npgsqlTransaction = (NpgsqlTransaction)transaction;

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {databaseSchema.MembershipPeriodsTableSql}
             SET left_at_ms = @left_at_ms, left_reason = @left_reason
             WHERE conversation_id = @conversation_id
               AND user_id = @user_id
               AND left_at_ms IS NULL;
             """,
            npgsqlConnection,
            npgsqlTransaction);

        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("left_at_ms", leftAtMs);
        command.Parameters.AddWithValue("left_reason", leftReason);

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 查询用户在指定会话中的所有 membership periods（按 joined_at_ms 排序）。
    /// </summary>
    public async Task<IReadOnlyList<MembershipPeriod>> GetMembershipPeriodsAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured)
            return Array.Empty<MembershipPeriod>();

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT joined_at_ms, left_at_ms, left_reason
             FROM {databaseSchema.MembershipPeriodsTableSql}
             WHERE conversation_id = @conversation_id AND user_id = @user_id
             ORDER BY joined_at_ms;
             """,
            connection);

        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);

        var periods = new List<MembershipPeriod>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            periods.Add(new MembershipPeriod
            {
                JoinedAtMs = reader.GetInt64(0),
                LeftAtMs = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                LeftReason = reader.IsDBNull(2) ? null : reader.GetString(2)
            });
        }

        return periods;
    }

    /// <summary>
    /// 六-4：账号清理时删除该用户的全部 membership periods。
    /// </summary>
    public async Task<int> DeleteByUserAsync(long userId, CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured)
            return 0;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {databaseSchema.MembershipPeriodsTableSql}
             WHERE user_id = @user_id;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 用户生命周期屏障 + 删除 tombstone 存储。
/// <para>
/// Tombstone 在账号删除清理开始前写入（PK=user_id，ON CONFLICT DO NOTHING，state=Deleting），
/// 清理完成后更新为 state=Deleted。所有写入处理器在处理前检查状态，拒绝 Deleting/Deleted 用户的命令。
/// </para>
/// </summary>
public sealed class NpgsqlUserDeletionTombstoneStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema,
    ILogger<NpgsqlUserDeletionTombstoneStore> logger) : IUserDeletionTombstoneStore
{
    public async Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || userId <= 0)
            return false;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {databaseSchema.UserDeletionTombstonesTableSql}
             WHERE user_id = @user_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    public async Task<UserLifecycleState> GetLifecycleStateAsync(
        long userId,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || userId <= 0)
            return UserLifecycleState.Active;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT state
             FROM {databaseSchema.UserDeletionTombstonesTableSql}
             WHERE user_id = @user_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return UserLifecycleState.Active;

        var stateByte = reader.GetByte(0);
        return MapStateByte(stateByte);
    }

    public async Task<IReadOnlyDictionary<long, UserLifecycleState>> BatchGetUserLifecycleStateAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        if (userIds.Count == 0)
            return new Dictionary<long, UserLifecycleState>();

        // 默认所有用户为 Active，未在 tombstone 表中找到的用户保持 Active。
        var result = new Dictionary<long, UserLifecycleState>(userIds.Count);
        foreach (var id in userIds)
            result.TryAdd(id, UserLifecycleState.Active);

        if (!databaseClient.IsConfigured)
            return result;

        // 仅查询有效 userId，避免无意义扫描；去重后传入数组参数。
        var candidateIds = userIds.Where(id => id > 0).Distinct().ToArray();
        if (candidateIds.Length == 0)
            return result;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT user_id, state
             FROM {databaseSchema.UserDeletionTombstonesTableSql}
             WHERE user_id = ANY(@user_ids);
             """,
            connection);
        command.Parameters.AddWithValue("user_ids", candidateIds);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var userId = reader.GetInt64(0);
            var stateByte = reader.GetByte(1);
            result[userId] = MapStateByte(stateByte);
        }

        return result;
    }

    public async Task RecordDeletionAsync(
        long userId,
        string deletionEventId,
        long deletedAtMs,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || userId <= 0)
        {
            logger.LogDebug(
                "Tombstone record skipped (db not configured or invalid userId). user={UserId}",
                userId);
            return;
        }

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // P0-2：获取排他 advisory lock，等待该用户所有进行中的消息/群写入事务提交或回滚。
        // 排他锁释放（本事务提交）时 tombstone 已持久化，后续写入能读到 state=Deleting 并拒绝。
        await UserLifecycleAdvisoryLock.AcquireExclusiveAsync(
            connection,
            transaction,
            userId,
            ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.UserDeletionTombstonesTableSql}
                 (user_id, deletion_event_id, deleted_at_ms, state)
             VALUES (@user_id, @event_id, @deleted_at_ms, @state)
             ON CONFLICT (user_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("event_id", deletionEventId);
        command.Parameters.AddWithValue("deleted_at_ms", deletedAtMs);
        command.Parameters.AddWithValue("state", (byte)UserLifecycleState.Deleting);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        if (affected > 0)
        {
            logger.LogInformation(
                "用户删除 tombstone 已记录（state=Deleting）。user={UserId}; event={EventId}",
                userId,
                deletionEventId);
        }
    }

    public async Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || userId <= 0)
            return;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {databaseSchema.UserDeletionTombstonesTableSql}
             SET state = @state
             WHERE user_id = @user_id AND state = @deleting_state;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("state", (byte)UserLifecycleState.Deleted);
        command.Parameters.AddWithValue("deleting_state", (byte)UserLifecycleState.Deleting);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (affected > 0)
        {
            logger.LogInformation(
                "用户删除清理已完成（state=Deleted）。user={UserId}",
                userId);
        }
    }

    public async Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default)
    {
        // P0-4: tombstone 永久保留策略——用户 ID 永不复用，轻量 tombstone 永久保留，
        // 确保 Deleting/Deleted 状态不会被 GC 误删导致已注销用户重新开放写入。
        // 仅清理 state=Deleted 且超期的记录（保留 Deleting 状态用于排查卡死的清理流程）。
        batchSize = Math.Clamp(batchSize, 1, 10_000);
        if (!databaseClient.IsConfigured)
            return 0;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {databaseSchema.UserDeletionTombstonesTableSql}
             WHERE user_id IN (
                 SELECT user_id
                 FROM {databaseSchema.UserDeletionTombstonesTableSql}
                 WHERE deleted_at_ms < @cutoff
                   AND state = @deleted_state
                 LIMIT @batch_size
                 FOR UPDATE SKIP LOCKED
             );
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", cutoffMs);
        command.Parameters.AddWithValue("batch_size", batchSize);
        command.Parameters.AddWithValue("deleted_state", (byte)UserLifecycleState.Deleted);

        var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (deleted > 0)
        {
            logger.LogDebug(
                "Tombstone GC 已清理 {Count} 条已完成的过期记录。cutoff={Cutoff}",
                deleted,
                cutoffMs);
        }
        return deleted;
    }

    /// <summary>
    /// 三-1：将数据库 state 字节映射为 <see cref="UserLifecycleState"/>。
    /// <para>
    /// Frozen=3 必须显式映射，否则会被默认分支误判为 Active，
    /// 导致冻结用户通过 lifecycle 预检查（advisory lock 路径已正确处理，
    /// 但 IsUserDeletedAsync/GetLifecycleStateAsync 预检查路径会漏判）。
    /// </para>
    /// </summary>
    private static UserLifecycleState MapStateByte(byte stateByte) =>
        stateByte switch
        {
            1 => UserLifecycleState.Deleting,
            2 => UserLifecycleState.Deleted,
            3 => UserLifecycleState.Frozen,
            _ => UserLifecycleState.Active
        };
}

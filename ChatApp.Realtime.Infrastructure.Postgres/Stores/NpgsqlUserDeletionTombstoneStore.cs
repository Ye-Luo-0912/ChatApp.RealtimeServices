using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
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
        return stateByte switch
        {
            1 => UserLifecycleState.Deleting,
            2 => UserLifecycleState.Deleted,
            _ => UserLifecycleState.Active
        };
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

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.UserDeletionTombstonesTableSql}
                 (user_id, deletion_event_id, deleted_at_ms, state)
             VALUES (@user_id, @event_id, @deleted_at_ms, @state)
             ON CONFLICT (user_id) DO NOTHING;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("event_id", deletionEventId);
        command.Parameters.AddWithValue("deleted_at_ms", deletedAtMs);
        command.Parameters.AddWithValue("state", (byte)UserLifecycleState.Deleting);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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
                 LIMIT @batch_size
                 FOR UPDATE SKIP LOCKED
             );
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", cutoffMs);
        command.Parameters.AddWithValue("batch_size", batchSize);

        var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (deleted > 0)
        {
            logger.LogDebug(
                "Tombstone GC 已清理 {Count} 条过期记录。cutoff={Cutoff}",
                deleted,
                cutoffMs);
        }
        return deleted;
    }
}

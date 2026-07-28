using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 账号清理 Saga 作业存储。
/// <para>
/// 所有写操作使用单条 SQL + FOR UPDATE SKIP LOCKED 保证原子性，
/// 不依赖外部分布式锁。TryClaim / GetNextPending 通过状态从 pending 翻转为 running 实现认领。
/// </para>
/// </summary>
public sealed class NpgsqlAccountCleanupJobStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema,
    ILogger<NpgsqlAccountCleanupJobStore> logger) : IAccountCleanupJobStore
{
    public async Task<AccountCleanupJob> EnqueueJobAsync(
        long userId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {databaseSchema.AccountCleanupJobsTableSql}
                (user_id, phase, cursor, status, retry_count, updated_at_ms)
            VALUES (@user_id, @phase, NULL, @status, 0, @updated_at_ms)
            ON CONFLICT (user_id, phase) DO NOTHING;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", AccountCleanupJob.PhaseAttachments);
        command.Parameters.AddWithValue("status", AccountCleanupJob.StatusPending);
        command.Parameters.AddWithValue("updated_at_ms", nowMs);

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        logger.LogInformation(
            "账号清理作业已入队。用户={UserId}；初始阶段={Phase}",
            userId,
            AccountCleanupJob.PhaseAttachments);

        return new AccountCleanupJob(
            userId,
            AccountCleanupJob.PhaseAttachments,
            Cursor: null,
            AccountCleanupJob.StatusPending,
            RetryCount: 0,
            UpdatedAtMs: nowMs);
    }

    public async Task<AccountCleanupJob?> TryClaimAsync(long userId, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET status = @running, updated_at_ms = @now_ms
            WHERE user_id = @user_id
              AND status = @pending
            RETURNING user_id, phase, cursor, status, retry_count, updated_at_ms;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        command.Parameters.AddWithValue("pending", AccountCleanupJob.StatusPending);
        command.Parameters.AddWithValue("now_ms", nowMs);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return null;
        }

        var job = Map(reader);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        logger.LogDebug(
            "账号清理作业已认领。用户={UserId}；阶段={Phase}",
            job.UserId,
            job.Phase);
        return job;
    }

    public async Task UpdateProgressAsync(
        long userId,
        string phase,
        string? cursor,
        string status,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentException.ThrowIfNullOrWhiteSpace(status);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET cursor = @cursor, status = @status, updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("cursor", (object?)cursor ?? DBNull.Value);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("now_ms", nowMs);

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task CompletePhaseAsync(long userId, string phase, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);

        var nextPhase = phase switch
        {
            AccountCleanupJob.PhaseAttachments => AccountCleanupJob.PhaseMetadata,
            AccountCleanupJob.PhaseMetadata => AccountCleanupJob.PhaseCompleted,
            _ => null
        };

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // 当前 phase 标记 completed。
        await using var completeCommand = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET status = @completed, cursor = NULL, updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase;
            """,
            connection,
            transaction);
        completeCommand.Parameters.AddWithValue("user_id", userId);
        completeCommand.Parameters.AddWithValue("phase", phase);
        completeCommand.Parameters.AddWithValue("completed", AccountCleanupJob.StatusCompleted);
        completeCommand.Parameters.AddWithValue("now_ms", nowMs);
        await completeCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // 下一 phase 置为 pending（若存在）。
        if (nextPhase is not null)
        {
            await using var nextCommand = new NpgsqlCommand(
                $"""
                INSERT INTO {databaseSchema.AccountCleanupJobsTableSql}
                    (user_id, phase, cursor, status, retry_count, updated_at_ms)
                VALUES (@user_id, @phase, NULL, @pending, 0, @now_ms)
                ON CONFLICT (user_id, phase) DO UPDATE
                    SET status = EXCLUDED.status,
                        cursor = EXCLUDED.cursor,
                        retry_count = EXCLUDED.retry_count,
                        updated_at_ms = EXCLUDED.updated_at_ms;
                """,
                connection,
                transaction);
            nextCommand.Parameters.AddWithValue("user_id", userId);
            nextCommand.Parameters.AddWithValue("phase", nextPhase);
            nextCommand.Parameters.AddWithValue("pending", AccountCleanupJob.StatusPending);
            nextCommand.Parameters.AddWithValue("now_ms", nowMs);
            await nextCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);

        logger.LogInformation(
            "账号清理阶段已完成。用户={UserId}；阶段={Phase}；下一阶段={NextPhase}",
            userId,
            phase,
            nextPhase ?? "(none)");
    }

    public async Task<AccountCleanupJob?> GetNextPendingAsync(CancellationToken ct = default)
    {
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET status = @running, updated_at_ms = @now_ms
            WHERE (user_id, phase) IN (
                SELECT user_id, phase
                FROM {databaseSchema.AccountCleanupJobsTableSql}
                WHERE status = @pending
                ORDER BY updated_at_ms
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING user_id, phase, cursor, status, retry_count, updated_at_ms;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        command.Parameters.AddWithValue("pending", AccountCleanupJob.StatusPending);
        command.Parameters.AddWithValue("now_ms", nowMs);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return null;
        }

        var job = Map(reader);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        logger.LogDebug(
            "账号清理作业已取出。用户={UserId}；阶段={Phase}",
            job.UserId,
            job.Phase);
        return job;
    }

    public async Task RecordFailureAsync(
        long userId,
        string phase,
        int maxRetryCount,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // retry_count++；若超过阈值则标记 failed，否则回退 pending 等待重试。
        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 > @max_retry THEN @failed ELSE @pending END,
                updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase
            RETURNING user_id, phase, cursor, status, retry_count, updated_at_ms;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("max_retry", maxRetryCount);
        command.Parameters.AddWithValue("failed", AccountCleanupJob.StatusFailed);
        command.Parameters.AddWithValue("pending", AccountCleanupJob.StatusPending);
        command.Parameters.AddWithValue("now_ms", nowMs);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var job = Map(reader);
            logger.LogWarning(
                "账号清理阶段失败。用户={UserId}；阶段={Phase}；重试次数={RetryCount}；状态={Status}",
                job.UserId,
                job.Phase,
                job.RetryCount,
                job.Status);
        }
    }

    private static AccountCleanupJob Map(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.IsDBNull(2) ? null : reader.GetString(2),
        reader.GetString(3),
        reader.GetInt32(4),
        reader.GetInt64(5));
}

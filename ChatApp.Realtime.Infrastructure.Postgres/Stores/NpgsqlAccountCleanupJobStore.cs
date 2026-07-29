using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 账号清理 Saga 作业存储。
/// <para>
/// 六-1：所有认领/进度/完成/失败操作均通过 claim_token 校验 lease 归属，防止旧 lease 误操作。
/// GetNextPendingAsync 在认领时写入租约（claim_token / locked_by / locked_until_ms），
/// lease 过期后 running 作业可被其他实例重新认领；RenewLeaseAsync 在批处理间续租。
/// </para>
/// <para>
/// 六-3：ProcessAttachmentsBatchAtomicAsync 在同一事务中完成 Outbox 入队、附件元数据删除、
/// cursor 更新三项操作，保证原子性。
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
        var claimToken = Guid.NewGuid().ToString("N");
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
        string claimToken,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentException.ThrowIfNullOrWhiteSpace(status);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimToken);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET cursor = @cursor, status = @status, updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase
              AND claim_token = @claim_token AND status = @running;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("cursor", (object?)cursor ?? DBNull.Value);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("claim_token", claimToken);
        command.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        command.Parameters.AddWithValue("now_ms", nowMs);

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task CompletePhaseAsync(
        long userId,
        string phase,
        string claimToken,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimToken);

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

        // 当前 phase 标记 completed，并清空 lease 字段。
        await using var completeCommand = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET status = @completed, cursor = NULL, updated_at_ms = @now_ms,
                claim_token = NULL, locked_by = NULL, locked_until_ms = NULL
            WHERE user_id = @user_id AND phase = @phase
              AND claim_token = @claim_token AND status = @running;
            """,
            connection,
            transaction);
        completeCommand.Parameters.AddWithValue("user_id", userId);
        completeCommand.Parameters.AddWithValue("phase", phase);
        completeCommand.Parameters.AddWithValue("completed", AccountCleanupJob.StatusCompleted);
        completeCommand.Parameters.AddWithValue("claim_token", claimToken);
        completeCommand.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
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
                        claim_token = NULL,
                        locked_by = NULL,
                        locked_until_ms = NULL,
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

    public async Task<AccountCleanupJob?> GetNextPendingAsync(
        string instanceId,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lockedUntilMs = nowMs + (long)leaseDuration.TotalMilliseconds;
        var claimToken = Guid.NewGuid().ToString("N");
        var maxRetries = 10; // 上限保护，retry_count 超过此值的 failed 作业不再被认领

        await using var command = new NpgsqlCommand(
            $"""
            WITH candidates AS (
                SELECT user_id, phase
                FROM {databaseSchema.AccountCleanupJobsTableSql}
                WHERE (status = @pending
                       OR (status = @running AND (locked_until_ms IS NULL OR locked_until_ms < @now_ms)))
                  AND retry_count < @max_retries
                ORDER BY updated_at_ms
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {databaseSchema.AccountCleanupJobsTableSql} AS item
            SET status = @running,
                claim_token = @claim_token,
                locked_by = @instance_id,
                locked_until_ms = @locked_until,
                updated_at_ms = @now_ms
            FROM candidates
            WHERE item.user_id = candidates.user_id AND item.phase = candidates.phase
            RETURNING item.user_id, item.phase, item.cursor, item.status, item.retry_count,
                item.updated_at_ms, item.claim_token, item.locked_by, item.locked_until_ms;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        command.Parameters.AddWithValue("pending", AccountCleanupJob.StatusPending);
        command.Parameters.AddWithValue("now_ms", nowMs);
        command.Parameters.AddWithValue("claim_token", claimToken);
        command.Parameters.AddWithValue("instance_id", instanceId);
        command.Parameters.AddWithValue("locked_until", lockedUntilMs);
        command.Parameters.AddWithValue("max_retries", maxRetries);

        AccountCleanupJob? job = null;
        await using (var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
                job = Map(reader);
        }

        if (job is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return null;
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);

        logger.LogDebug(
            "账号清理作业已取出。用户={UserId}；阶段={Phase}；lease 到期={LeaseUntilMs}",
            job.UserId,
            job.Phase,
            job.LockedUntilMs);
        return job;
    }

    public async Task<bool> RenewLeaseAsync(
        long userId,
        string phase,
        string claimToken,
        TimeSpan leaseExtension,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimToken);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lockedUntilMs = nowMs + (long)leaseExtension.TotalMilliseconds;
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET locked_until_ms = @locked_until,
                updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase
              AND claim_token = @claim_token
              AND status = @running;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("claim_token", claimToken);
        command.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        command.Parameters.AddWithValue("locked_until", lockedUntilMs);
        command.Parameters.AddWithValue("now_ms", nowMs);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return affected > 0;
    }

    public async Task<bool> ProcessAttachmentsBatchAtomicAsync(
        long userId,
        string claimToken,
        string lastAttachmentId,
        IReadOnlyList<string> attachmentIds,
        RealtimeEvent purgeEvent,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(claimToken);
        ArgumentException.ThrowIfNullOrWhiteSpace(lastAttachmentId);
        ArgumentNullException.ThrowIfNull(attachmentIds);
        ArgumentNullException.ThrowIfNull(purgeEvent);

        if (attachmentIds.Count == 0)
            return true;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // 六-3 step 1：在同一事务中写入 purge Outbox 事件（幂等 ON CONFLICT DO NOTHING）。
        await OutboxInsertHelper.InsertManyAsync(
            connection,
            transaction,
            databaseSchema,
            new[] { purgeEvent },
            ct).ConfigureAwait(false);

        // 六-3 step 2：删除本批附件元数据。
        await using var deleteCmd = new NpgsqlCommand(
            $"""
            DELETE FROM {databaseSchema.AttachmentsTableSql}
            WHERE attachment_id = ANY(@ids);
            """,
            connection,
            transaction);
        var idsParam = deleteCmd.Parameters.Add(
            "ids",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        idsParam.Value = attachmentIds.ToArray();
        await deleteCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // 六-3 step 3：更新 Job cursor（带 claim_token 校验）。
        await using var cursorCmd = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET cursor = @cursor, updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase
              AND claim_token = @claim_token AND status = @running;
            """,
            connection,
            transaction);
        cursorCmd.Parameters.AddWithValue("user_id", userId);
        cursorCmd.Parameters.AddWithValue("phase", AccountCleanupJob.PhaseAttachments);
        cursorCmd.Parameters.AddWithValue("cursor", lastAttachmentId);
        cursorCmd.Parameters.AddWithValue("claim_token", claimToken);
        cursorCmd.Parameters.AddWithValue("running", AccountCleanupJob.StatusRunning);
        cursorCmd.Parameters.AddWithValue("now_ms", nowMs);

        var affected = await cursorCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (affected == 0)
        {
            // lease 已失效（被抢占或过期），回滚本批 Outbox + 附件删除。
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            logger.LogWarning(
                "账号清理 attachments 批次 lease 失效，已回滚。用户={UserId}；cursor={Cursor}",
                userId,
                lastAttachmentId);
            return false;
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return true;
    }

    public async Task RecordFailureAsync(
        long userId,
        string phase,
        string claimToken,
        int maxRetryCount,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(phase);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimToken);

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // retry_count++；若超过阈值则标记 failed，否则回退 pending 等待重试。
        // 六-1：清空 lease 字段，使 pending 作业可被重新认领。
        await using var command = new NpgsqlCommand(
            $"""
            UPDATE {databaseSchema.AccountCleanupJobsTableSql}
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 > @max_retry THEN @failed ELSE @pending END,
                claim_token = NULL,
                locked_by = NULL,
                locked_until_ms = NULL,
                updated_at_ms = @now_ms
            WHERE user_id = @user_id AND phase = @phase
              AND claim_token = @claim_token
            RETURNING user_id, phase, cursor, status, retry_count, updated_at_ms,
                claim_token, locked_by, locked_until_ms;
            """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("claim_token", claimToken);
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
        reader.GetInt64(5),
        reader.IsDBNull(6) ? null : reader.GetString(6),
        reader.IsDBNull(7) ? null : reader.GetString(7),
        reader.IsDBNull(8) ? null : reader.GetInt64(8));
}

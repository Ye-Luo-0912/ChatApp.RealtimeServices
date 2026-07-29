using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 六-1：为 account_cleanup_jobs 表添加 lease 字段，支持 Job 租约（claim/续租/过期重新认领）。
/// <para>
/// 原实现将 pending 翻转为 running 后无租约，崩溃后永远停留在 running 不再被认领。
/// 新增 claim_token / locked_by / locked_until_ms 三列：GetNextPendingAsync 在认领时
/// 写入租约，RenewLeaseAsync 续租，lease 过期后 running 作业可被其他实例重新认领。
/// </para>
/// </summary>
public sealed class Migration041_AccountCleanupJobLease : IRealtimeSchemaMigration
{
    public int Version => 41;
    public string Name => "account_cleanup_job_lease";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var jobs = schema.AccountCleanupJobsTableSql;

        await using var alterCmd = new NpgsqlCommand(
            $"""
            ALTER TABLE {jobs}
            ADD COLUMN IF NOT EXISTS "claim_token" text NULL,
            ADD COLUMN IF NOT EXISTS "locked_by" character varying(128) NULL,
            ADD COLUMN IF NOT EXISTS "locked_until_ms" bigint NULL;
            """,
            connection,
            transaction);
        await alterCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // 索引：用于查找 lease 过期的 running 作业（locked_until_ms < now）。
        await using var idxCmd = new NpgsqlCommand(
            $"""
            CREATE INDEX IF NOT EXISTS "ix_account_cleanup_jobs_locked_until"
                ON {jobs} ("locked_until_ms")
                WHERE "locked_until_ms" IS NOT NULL;
            """,
            connection,
            transaction);
        await idxCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// LongTerm-2：账号清理可续跑 Saga 的作业表。
/// <para>
/// 新增 <c>account_cleanup_jobs</c> 表，按 (user_id, phase) 跟踪清理进度：
/// cursor 记录最后处理的附件 key（断点续跑），status 控制 pending/running/completed/failed，
/// retry_count 限制重试次数。AccountCleanupWorker 轮询本表，按 phase 分批推进清理。
/// </para>
/// <para>
/// 表结构故意精简：phase 与 status 使用 varchar 而非 enum，避免迁移期 enum 依赖；
/// cursor 为 varchar(256) 以容纳 object_key 等字符串游标。
/// </para>
/// </summary>
public sealed class Migration036_AccountCleanupJobs : IRealtimeSchemaMigration
{
    public int Version => 36;
    public string Name => "account_cleanup_jobs";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var jobs = schema.AccountCleanupJobsTableSql;

        await using var createTable = new NpgsqlCommand(
            $"""
            CREATE TABLE IF NOT EXISTS {jobs} (
                "user_id" bigint NOT NULL,
                "phase" character varying(32) NOT NULL,
                "cursor" character varying(256) NULL,
                "status" character varying(16) NOT NULL DEFAULT 'pending',
                "retry_count" integer NOT NULL DEFAULT 0,
                "updated_at_ms" bigint NOT NULL,
                PRIMARY KEY ("user_id", "phase")
            );
            """,
            connection,
            transaction);
        await createTable.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // 按状态 + 更新时间索引，使 GetNextPending 仅扫描待处理作业，避免全表扫描。
        await using var statusIndex = new NpgsqlCommand(
            $"""
            CREATE INDEX IF NOT EXISTS "ix_account_cleanup_jobs_status_updated_at"
                ON {jobs} ("status", "updated_at_ms");
            """,
            connection,
            transaction);
        await statusIndex.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

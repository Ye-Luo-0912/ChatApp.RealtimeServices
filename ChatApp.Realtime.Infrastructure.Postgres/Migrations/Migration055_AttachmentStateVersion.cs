using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// P1-3：附件 state_version 列 + 状态 CHECK 约束放宽至 (0..8)。
/// <para>
/// state_version 用于每次状态转换的条件更新（ABA 防护），防止旧扫描结果覆盖新状态。
/// 新增状态 Available(7)/Expired(8) 纳入 CHECK 约束，并建立过期候选索引。
/// </para>
/// </summary>
public sealed class Migration055_AttachmentStateVersion : IRealtimeSchemaMigration
{
    public int Version => 55;
    public string Name => "attachment_state_version";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var attachments = schema.AttachmentsTableSql;

        var commands = new[]
        {
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "state_version" bigint NOT NULL DEFAULT 0;
             """,
            $"""
             ALTER TABLE {attachments}
             DROP CONSTRAINT IF EXISTS "ck_attachments_status_known";
             """,
            $"""
             ALTER TABLE {attachments}
             ADD CONSTRAINT "ck_attachments_status_known"
             CHECK ("status" IN (0, 1, 2, 3, 4, 5, 6, 7, 8));
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_attachments_expiry_candidates"
             ON {attachments} ("status", "created_at_ms")
             WHERE "message_id" IS NULL AND "status" IN (0, 4, 5);
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
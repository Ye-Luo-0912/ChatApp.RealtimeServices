using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 放宽 attachments.status CHECK 约束至 (0..6)，对齐 AttachmentStatus 全部 7 个状态
/// （Ticketed/Confirmed/Bound/Abandoned/Uploaded/Scanning/Rejected）。
/// Migration012 原始约束仅允许 (0,1,2,3)，阻止 FinalizeUpload 路径写入
/// Uploaded(4)/Scanning(5)/Rejected(6)。本迁移 DROP 旧约束并 ADD 新约束。
/// </summary>
public sealed class Migration051_AttachmentStatusExtended : IRealtimeSchemaMigration
{
    public int Version => 51;
    public string Name => "attachment_status_extended";

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
             DROP CONSTRAINT IF EXISTS "ck_attachments_status_known";
             """,
            $"""
             ALTER TABLE {attachments}
             ADD CONSTRAINT "ck_attachments_status_known"
             CHECK ("status" IN (0, 1, 2, 3, 4, 5, 6));
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
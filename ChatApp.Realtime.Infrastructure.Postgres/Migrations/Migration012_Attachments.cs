using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 正式附件元数据表：Ticketed �?Confirmed �?Bound（或 Abandoned）�?
/// </summary>
public sealed class Migration012_Attachments : IRealtimeSchemaMigration
{
    public int Version => 12;
    public string Name => "attachments";

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
             CREATE TABLE IF NOT EXISTS {attachments} (
                 "attachment_id" character varying(64) NOT NULL PRIMARY KEY,
                 "uploader_user_id" bigint NOT NULL,
                 "object_key" character varying(512) NOT NULL,
                 "public_url" character varying(1024) NULL,
                 "content_type" character varying(128) NOT NULL,
                 "size_bytes" bigint NOT NULL,
                 "original_name" character varying(256) NULL,
                 "status" smallint NOT NULL,
                 "message_id" character varying(64) NULL,
                 "conversation_id" character varying(64) NULL,
                 "client_attachment_id" character varying(128) NULL,
                 "created_at_ms" bigint NOT NULL,
                 "confirmed_at_ms" bigint NULL,
                 "bound_at_ms" bigint NULL,
                 CONSTRAINT ck_attachments_uploader_positive CHECK ("uploader_user_id" > 0),
                 CONSTRAINT ck_attachments_size_nonnegative CHECK ("size_bytes" >= 0),
                 CONSTRAINT ck_attachments_status_known CHECK ("status" IN (0, 1, 2, 3))
             );
             """,
            $"""
             CREATE UNIQUE INDEX IF NOT EXISTS "ux_attachments_object_key"
             ON {attachments} ("object_key");
             """,
            $"""
             CREATE UNIQUE INDEX IF NOT EXISTS "ux_attachments_uploader_client"
             ON {attachments} ("uploader_user_id", "client_attachment_id")
             WHERE "client_attachment_id" IS NOT NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_attachments_message"
             ON {attachments} ("message_id")
             WHERE "message_id" IS NOT NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_attachments_uploader_status"
             ON {attachments} ("uploader_user_id", "status", "created_at_ms");
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_attachments_unbound_age"
             ON {attachments} ("created_at_ms")
             WHERE "status" IN (0, 1);
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

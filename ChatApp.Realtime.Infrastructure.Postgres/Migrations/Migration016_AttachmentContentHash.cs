using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>附件 content_hash（上传/扫描 SHA-256 十六进制）。</summary>
public sealed class Migration016_AttachmentContentHash : IRealtimeSchemaMigration
{
    public int Version => 16;
    public string Name => "attachment_content_hash";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.AttachmentsTableSql}
             ADD COLUMN IF NOT EXISTS "content_hash" character varying(64) NULL;
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

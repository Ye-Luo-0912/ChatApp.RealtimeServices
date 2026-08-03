using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Target-side fence for Server attachment scan projections. A projection may
/// update metadata only when its generation is not older than the last applied
/// generation for that attachment.
/// </summary>
public sealed class Migration050_AttachmentProjectionFence : IRealtimeSchemaMigration
{
    public int Version => 50;
    public string Name => "attachment_projection_fence";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.AttachmentsTableSql}
             ADD COLUMN IF NOT EXISTS "scan_projection_id" bigint NULL;

             ALTER TABLE {schema.AttachmentsTableSql}
             ADD COLUMN IF NOT EXISTS "scan_version" bigint NOT NULL DEFAULT 0;
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

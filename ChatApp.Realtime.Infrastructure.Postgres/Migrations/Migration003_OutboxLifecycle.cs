using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Outbox 生命周期：显�?status（Pending/Published/Dead）、死信可查询，以及已发布行清理索引�?
/// </summary>
public sealed class Migration003_OutboxLifecycle : IRealtimeSchemaMigration
{
    public int Version => 3;
    public string Name => "outbox_lifecycle";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;
        var quotedSchema = schema.QuotedSchema;

        var commands = new[]
        {
            $"ALTER TABLE {outbox} ADD COLUMN IF NOT EXISTS \"status\" smallint NULL;",
            $"""
             UPDATE {outbox}
             SET "status" = CASE
                 WHEN "published_at_ms" IS NOT NULL THEN 1
                 ELSE 0
             END
             WHERE "status" IS NULL;
             """,
            $"ALTER TABLE {outbox} ALTER COLUMN \"status\" SET DEFAULT 0;",
            $"ALTER TABLE {outbox} ALTER COLUMN \"status\" SET NOT NULL;",
            $"DROP INDEX IF EXISTS {quotedSchema}.\"ix_outbox_pending\";",
            $"""
             CREATE INDEX IF NOT EXISTS "ix_outbox_pending"
             ON {outbox} ("next_attempt_at_ms", "created_at_ms")
             WHERE "status" = 0;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_outbox_dead"
             ON {outbox} ("created_at_ms")
             WHERE "status" = 2;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_outbox_published_cleanup"
             ON {outbox} ("published_at_ms")
             WHERE "status" = 1;
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

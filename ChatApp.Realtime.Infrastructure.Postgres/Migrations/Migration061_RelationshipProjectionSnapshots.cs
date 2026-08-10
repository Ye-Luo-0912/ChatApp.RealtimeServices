using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public sealed class Migration061_RelationshipProjectionSnapshots : IRealtimeSchemaMigration
{
    public int Version => 61;
    public string Name => "relationship_projection_snapshots";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var commands = new[]
        {
            $"""
             ALTER TABLE {schema.RelationshipProjectionItemsTableSql}
                 DROP CONSTRAINT IF EXISTS "ck_relationship_projection_items_version_positive";
             ALTER TABLE {schema.RelationshipProjectionItemsTableSql}
                 ADD CONSTRAINT "ck_relationship_projection_items_version_non_negative"
                 CHECK ("version" >= 0);
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionSnapshotsTableSql} (
                 "snapshot_id" character varying(64) NOT NULL PRIMARY KEY,
                 "owner_user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "version" bigint NOT NULL,
                 "item_count" integer NOT NULL,
                 "resource_hash" character varying(64) NOT NULL,
                 "captured_at_ms" bigint NOT NULL,
                 "applied_at_ms" bigint NOT NULL,
                 CONSTRAINT "ck_relationship_projection_snapshots_list_type"
                     CHECK ("list_type" IN (1, 2, 3)),
                 CONSTRAINT "ck_relationship_projection_snapshots_non_negative"
                     CHECK ("version" >= 0 AND "item_count" >= 0),
                 UNIQUE ("owner_user_id", "list_type", "version", "resource_hash")
             );
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_relationship_projection_snapshots_stream"
             ON {schema.RelationshipProjectionSnapshotsTableSql}
                 ("owner_user_id", "list_type", "version" DESC);
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

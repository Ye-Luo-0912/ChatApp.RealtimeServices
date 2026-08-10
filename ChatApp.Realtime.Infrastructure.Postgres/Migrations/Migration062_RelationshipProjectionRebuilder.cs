using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public sealed class Migration062_RelationshipProjectionRebuilder : IRealtimeSchemaMigration
{
    public int Version => 62;
    public string Name => "relationship_projection_rebuilder";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionRebuildStateTableSql} (
                "id" smallint NOT NULL PRIMARY KEY,
                "after_owner_user_id" bigint NULL,
                "after_list_type" smallint NULL,
                "pass_number" bigint NOT NULL DEFAULT 0,
                "pass_changed" boolean NOT NULL DEFAULT FALSE,
                "stable_passes" integer NOT NULL DEFAULT 0,
                "lease_owner" character varying(128) NULL,
                "claim_token" character varying(32) NULL,
                "locked_until_ms" bigint NULL,
                "next_attempt_at_ms" bigint NOT NULL DEFAULT 0,
                "last_error" character varying(128) NULL,
                "updated_at_ms" bigint NOT NULL,
                CONSTRAINT "ck_relationship_projection_rebuild_singleton" CHECK ("id" = 1),
                CONSTRAINT "ck_relationship_projection_rebuild_cursor_pair" CHECK (
                    ("after_owner_user_id" IS NULL) = ("after_list_type" IS NULL)),
                CONSTRAINT "ck_relationship_projection_rebuild_list_type" CHECK (
                    "after_list_type" IS NULL OR "after_list_type" IN (1, 2, 3)),
                CONSTRAINT "ck_relationship_projection_rebuild_lease_pair" CHECK (
                    ("lease_owner" IS NULL) = ("claim_token" IS NULL)
                    AND ("lease_owner" IS NULL) = ("locked_until_ms" IS NULL)),
                CONSTRAINT "ck_relationship_projection_rebuild_non_negative" CHECK (
                    "pass_number" >= 0 AND "stable_passes" >= 0
                    AND "next_attempt_at_ms" >= 0 AND "updated_at_ms" >= 0)
            );

            INSERT INTO {schema.RelationshipProjectionRebuildStateTableSql}
                ("id", "updated_at_ms")
            VALUES (1, 0)
            ON CONFLICT ("id") DO NOTHING;
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

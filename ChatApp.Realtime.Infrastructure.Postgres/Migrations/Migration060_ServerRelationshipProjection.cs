using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Server-authoritative relationship read model. The inbox, item and stream version tables
/// are updated atomically before the owning Outbox event may be published.
/// </summary>
public sealed class Migration060_ServerRelationshipProjection : IRealtimeSchemaMigration
{
    public int Version => 60;
    public string Name => "server_relationship_projection";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionVersionsTableSql} (
                 "owner_user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "current_version" bigint NOT NULL,
                 "updated_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("owner_user_id", "list_type"),
                 CONSTRAINT "ck_relationship_projection_versions_list_type"
                     CHECK ("list_type" IN (1, 2, 3)),
                 CONSTRAINT "ck_relationship_projection_versions_non_negative"
                     CHECK ("current_version" >= 0)
             );
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionItemsTableSql} (
                 "owner_user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "resource_id" character varying(128) NOT NULL,
                 "subject_user_id" bigint NOT NULL,
                 "actor_user_id" bigint NOT NULL,
                 "state" character varying(32) NULL,
                 "message" character varying(512) NULL,
                 "version" bigint NOT NULL,
                 "occurred_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("owner_user_id", "list_type", "resource_id"),
                 CONSTRAINT "ck_relationship_projection_items_list_type"
                     CHECK ("list_type" IN (1, 2, 3)),
                 CONSTRAINT "ck_relationship_projection_items_version_positive"
                     CHECK ("version" > 0)
             );
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_relationship_projection_items_subject"
             ON {schema.RelationshipProjectionItemsTableSql}
                 ("owner_user_id", "list_type", "subject_user_id");
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionInboxTableSql} (
                 "event_id" character varying(64) NOT NULL PRIMARY KEY,
                 "owner_user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "version" bigint NOT NULL,
                 "processed_at_ms" bigint NOT NULL,
                 CONSTRAINT "ck_relationship_projection_inbox_list_type"
                     CHECK ("list_type" IN (1, 2, 3)),
                 CONSTRAINT "ck_relationship_projection_inbox_version_positive"
                     CHECK ("version" > 0)
             );
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_relationship_projection_inbox_stream"
             ON {schema.RelationshipProjectionInboxTableSql}
                 ("owner_user_id", "list_type", "version");
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

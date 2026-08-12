using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Explicit versioned change history per (owner, list). Each applied Server delta is
/// appended here in the same transaction as the item, inbox and stream version advance,
/// so catch-up can converge from a snapshot without replaying the legacy relationship
/// tables. The (owner, list, version) primary key guarantees a duplicate event never
/// produces a second history entry.
/// </summary>
public sealed class Migration063_RelationshipProjectionHistory : IRealtimeSchemaMigration
{
    public int Version => 63;
    public string Name => "relationship_projection_history";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {schema.RelationshipProjectionHistoryTableSql} (
                 "owner_user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "version" bigint NOT NULL,
                 "event_id" character varying(64) NOT NULL,
                 "operation" smallint NOT NULL,
                 "resource_id" character varying(128) NOT NULL,
                 "subject_user_id" bigint NOT NULL,
                 "actor_user_id" bigint NOT NULL,
                 "state" character varying(32) NULL,
                 "message" character varying(512) NULL,
                 "occurred_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("owner_user_id", "list_type", "version"),
                 CONSTRAINT "ck_relationship_projection_history_list_type"
                     CHECK ("list_type" IN (1, 2, 3)),
                 CONSTRAINT "ck_relationship_projection_history_operation"
                     CHECK ("operation" IN (1, 2)),
                 CONSTRAINT "ck_relationship_projection_history_version_positive"
                     CHECK ("version" > 0)
             );
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 关系列表增量同步游标表：按 (user_id, device_id_hash, list_type) 维度持久化设备水位。
/// </summary>
public sealed class Migration053_RelationshipSyncCursors : IRealtimeSchemaMigration
{
    public int Version => 53;
    public string Name => "relationship_sync_cursors";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var schemaSql = schema.QuotedSchema;

        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {schemaSql}."relationship_sync_cursors" (
                 "user_id" bigint NOT NULL,
                 "device_id_hash" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "after_changed_at_ms" bigint NOT NULL,
                 "updated_at_ms" bigint NOT NULL,
                 "last_seen_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("user_id", "device_id_hash", "list_type")
             );
             """,
            $"""CREATE INDEX IF NOT EXISTS "ix_relationship_sync_cursors_user" ON {schemaSql}."relationship_sync_cursors" ("user_id");""",
            $"""CREATE INDEX IF NOT EXISTS "ix_relationship_sync_cursors_last_seen" ON {schemaSql}."relationship_sync_cursors" ("last_seen_at_ms");"""
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
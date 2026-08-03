using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 关系变更日志表 + 全局单调序列：以 change_sequence 支撑关系增量同步。
/// </summary>
public sealed class Migration054_RelationshipChangeLog : IRealtimeSchemaMigration
{
    public int Version => 54;
    public string Name => "relationship_change_log";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var schemaSql = schema.QuotedSchema;

        var commands = new[]
        {
            $"""CREATE SEQUENCE IF NOT EXISTS {schemaSql}."relationship_change_seq";""",
            $"""
             CREATE TABLE IF NOT EXISTS {schemaSql}."relationship_change_log" (
                 "change_sequence" bigint NOT NULL,
                 "user_id" bigint NOT NULL,
                 "list_type" smallint NOT NULL,
                 "operation" smallint NOT NULL,
                 "resource_id" character varying(64) NOT NULL,
                 "status" character varying(32) NULL,
                 "message" character varying(512) NULL,
                 "created_at_ms" bigint NOT NULL,
                 "occurred_at_ms" bigint NOT NULL,
                 "request_id" character varying(64) NULL,
                 PRIMARY KEY ("change_sequence")
             );
             """,
            $"""CREATE INDEX IF NOT EXISTS "ix_relationship_change_log_query" ON {schemaSql}."relationship_change_log" ("user_id", "list_type", "change_sequence");""",
            $"""CREATE INDEX IF NOT EXISTS "ix_relationship_change_log_floor" ON {schemaSql}."relationship_change_log" ("user_id", "list_type", "occurred_at_ms");"""
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 关系域表：friend_requests / friendships / relationship_mutation_requests。
/// <para>
/// 黑名单复用既有 public."T_BlockRecords" 表，不新建。
/// </para>
/// </summary>
public sealed class Migration052_Relationships : IRealtimeSchemaMigration
{
    public int Version => 52;
    public string Name => "relationships";

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
             CREATE TABLE IF NOT EXISTS {schemaSql}."friend_requests" (
                 "request_id" character varying(64) NOT NULL,
                 "requester_id" bigint NOT NULL,
                 "target_id" bigint NOT NULL,
                 "message" character varying(512) NULL,
                 "status" smallint NOT NULL DEFAULT 0,
                 "created_at_ms" bigint NOT NULL,
                 "responded_at_ms" bigint NULL,
                 PRIMARY KEY ("request_id")
             );
             """,
            $"""CREATE INDEX IF NOT EXISTS "ix_friend_requests_target_pending" ON {schemaSql}."friend_requests" ("target_id") WHERE "status" = 0;""",
            $"""CREATE INDEX IF NOT EXISTS "ix_friend_requests_requester" ON {schemaSql}."friend_requests" ("requester_id", "status");""",
            $"""
             CREATE TABLE IF NOT EXISTS {schemaSql}."friendships" (
                 "friendship_id" character varying(64) NOT NULL,
                 "user_id_low" bigint NOT NULL,
                 "user_id_high" bigint NOT NULL,
                 "created_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("friendship_id"),
                 CONSTRAINT "uq_friendships_pair" UNIQUE ("user_id_low", "user_id_high")
             );
             """,
            $"""CREATE INDEX IF NOT EXISTS "ix_friendships_user_low" ON {schemaSql}."friendships" ("user_id_low");""",
            $"""CREATE INDEX IF NOT EXISTS "ix_friendships_user_high" ON {schemaSql}."friendships" ("user_id_high");""",
            $"""
             CREATE TABLE IF NOT EXISTS {schemaSql}."relationship_mutation_requests" (
                 "actor_user_id" bigint NOT NULL,
                 "request_id" character varying(64) NOT NULL,
                 "operation" smallint NOT NULL,
                 "request_fingerprint" character varying(64) NOT NULL,
                 "resource_id" character varying(64) NULL,
                 "succeeded" boolean NOT NULL,
                 "error_code" character varying(64) NULL,
                 "created_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("actor_user_id", "request_id")
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
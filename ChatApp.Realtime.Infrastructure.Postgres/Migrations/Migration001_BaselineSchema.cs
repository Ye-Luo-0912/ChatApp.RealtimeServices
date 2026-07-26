using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 基线表结构（messages / outbox）。对已由历史 ad-hoc DDL 创建的库使用 IF NOT EXISTS，可安全重入�?
/// </summary>
public sealed class Migration001_BaselineSchema : IRealtimeSchemaMigration
{
    public int Version => 1;
    public string Name => "baseline_schema";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var quotedSchema = schema.QuotedSchema;
        var messages = schema.MessagesTableSql;
        var outbox = schema.OutboxTableSql;

        var commands = new[]
        {
            $"CREATE SCHEMA IF NOT EXISTS {quotedSchema};",
            $"""
             CREATE TABLE IF NOT EXISTS {messages} (
                 "message_id" character varying(64) NOT NULL PRIMARY KEY,
                 "client_message_id" character varying(128) NOT NULL,
                 "sender_user_id" bigint NOT NULL,
                 "sender_session_id" character varying(128) NOT NULL,
                 "receiver_user_id" bigint NOT NULL,
                 "content" text NOT NULL,
                 "received_at_ms" bigint NOT NULL,
                 "delivered_at_ms" bigint NULL,
                 "read_at_ms" bigint NULL,
                 "created_at_ms" bigint NOT NULL
             );
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {outbox} (
                 "event_id" character varying(64) NOT NULL PRIMARY KEY,
                 "payload_json" text NOT NULL,
                 "created_at_ms" bigint NOT NULL,
                 "next_attempt_at_ms" bigint NOT NULL,
                 "published_at_ms" bigint NULL,
                 "attempt_count" integer NOT NULL DEFAULT 0,
                 "locked_by" character varying(128) NULL,
                 "locked_until_ms" bigint NULL,
                 "last_error" character varying(2048) NULL
             );
             """,
            $"ALTER TABLE {messages} ADD COLUMN IF NOT EXISTS \"delivered_at_ms\" bigint NULL;",
            $"ALTER TABLE {messages} ADD COLUMN IF NOT EXISTS \"read_at_ms\" bigint NULL;",
            $"CREATE UNIQUE INDEX IF NOT EXISTS \"ux_messages_sender_client_message\" ON {messages} (\"sender_user_id\", \"client_message_id\");",
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_receiver_history\" ON {messages} (\"receiver_user_id\", \"received_at_ms\" DESC, \"message_id\" DESC);",
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_sender_history\" ON {messages} (\"sender_user_id\", \"received_at_ms\" DESC, \"message_id\" DESC);",
            $"DROP INDEX IF EXISTS {quotedSchema}.\"ix_messages_receiver_received\";",
            $"DROP INDEX IF EXISTS {quotedSchema}.\"ix_messages_sender_received\";",
            $"ALTER TABLE {outbox} ALTER COLUMN \"attempt_count\" SET DEFAULT 0;",
            $"CREATE INDEX IF NOT EXISTS \"ix_outbox_pending\" ON {outbox} (\"next_attempt_at_ms\", \"created_at_ms\") WHERE \"published_at_ms\" IS NULL;"
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

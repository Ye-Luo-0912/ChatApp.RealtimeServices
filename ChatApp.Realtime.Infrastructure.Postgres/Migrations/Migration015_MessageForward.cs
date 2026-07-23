using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>消息转发引用字段（forwarded_from_*）。</summary>
public sealed class Migration015_MessageForward : IRealtimeSchemaMigration
{
    public int Version => 15;
    public string Name => "message_forward";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var commands = new[]
        {
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "forwarded_from_message_id" character varying(64) NULL;
             """,
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "forwarded_from_sender_user_id" bigint NULL;
             """,
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "forwarded_from_preview" character varying(256) NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_forwarded_from"
             ON {messages} ("forwarded_from_message_id")
             WHERE "forwarded_from_message_id" IS NOT NULL;
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

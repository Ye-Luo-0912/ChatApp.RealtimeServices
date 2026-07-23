using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>消息回复引用字段（reply_to_*）。</summary>
public sealed class Migration013_MessageReply : IRealtimeSchemaMigration
{
    public int Version => 13;
    public string Name => "message_reply";

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
             ADD COLUMN IF NOT EXISTS "reply_to_message_id" character varying(64) NULL;
             """,
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "reply_to_sender_user_id" bigint NULL;
             """,
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "reply_to_preview" character varying(256) NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_reply_to"
             ON {messages} ("reply_to_message_id")
             WHERE "reply_to_message_id" IS NOT NULL;
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

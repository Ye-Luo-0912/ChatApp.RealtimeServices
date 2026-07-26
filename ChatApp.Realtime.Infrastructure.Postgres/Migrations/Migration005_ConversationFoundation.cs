using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 会话基础模型：conversations / conversation_members、messages.conversation_id 与索引�?
/// 历史数据回填�?<see cref="Migration009_ConversationBackfillBatches"/>（分批、幂等）�?
/// </summary>
public sealed class Migration005_ConversationFoundation : IRealtimeSchemaMigration
{
    public int Version => 5;
    public string Name => "conversation_foundation";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var conversations = schema.ConversationsTableSql;
        var members = schema.ConversationMembersTableSql;

        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {conversations} (
                 "conversation_id" character varying(64) NOT NULL PRIMARY KEY,
                 "type" smallint NOT NULL,
                 "created_at_ms" bigint NOT NULL,
                 "updated_at_ms" bigint NOT NULL,
                 "last_message_id" character varying(64) NULL,
                 "last_message_preview" character varying(256) NULL,
                 "last_message_at_ms" bigint NULL,
                 "last_sender_user_id" bigint NULL,
                 CONSTRAINT "ck_conversations_type_known" CHECK ("type" IN (1, 2))
             );
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {members} (
                 "conversation_id" character varying(64) NOT NULL,
                 "user_id" bigint NOT NULL,
                 "peer_user_id" bigint NULL,
                 "joined_at_ms" bigint NOT NULL,
                 "last_read_message_id" character varying(64) NULL,
                 "last_read_at_ms" bigint NULL,
                 "unread_count" integer NOT NULL DEFAULT 0,
                 PRIMARY KEY ("conversation_id", "user_id"),
                 CONSTRAINT "ck_conversation_members_user_positive" CHECK ("user_id" > 0),
                 CONSTRAINT "ck_conversation_members_unread_nonnegative" CHECK ("unread_count" >= 0)
             );
             """,
            $"ALTER TABLE {messages} ADD COLUMN IF NOT EXISTS \"conversation_id\" character varying(64) NULL;",
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_conversation_history"
                 ON {messages} ("conversation_id", "received_at_ms" DESC, "message_id" DESC)
                 WHERE "conversation_id" IS NOT NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_conversation_members_user_list"
                 ON {members} ("user_id", "conversation_id");
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

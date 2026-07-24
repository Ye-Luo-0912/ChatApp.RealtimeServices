using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 群聊基础：会话标题 / 创建者、成员角色。在线友好（ADD COLUMN IF NOT EXISTS）。
/// </summary>
public sealed class Migration019_GroupConversationRoles : IRealtimeSchemaMigration
{
    public int Version => 19;
    public string Name => "group_conversation_roles";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var conversations = schema.ConversationsTableSql;
        var members = schema.ConversationMembersTableSql;

        var commands = new[]
        {
            $"""
             ALTER TABLE {conversations}
                 ADD COLUMN IF NOT EXISTS "title" character varying(128) NULL;
             """,
            $"""
             ALTER TABLE {conversations}
                 ADD COLUMN IF NOT EXISTS "created_by_user_id" bigint NULL;
             """,
            $"""
             ALTER TABLE {members}
                 ADD COLUMN IF NOT EXISTS "role" smallint NOT NULL DEFAULT 3;
             """,
            $"""
             ALTER TABLE {members}
                 DROP CONSTRAINT IF EXISTS "ck_conversation_members_role_known";
             """,
            $"""
             ALTER TABLE {members}
                 ADD CONSTRAINT "ck_conversation_members_role_known"
                 CHECK ("role" IN (1, 2, 3));
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 会话成员偏好表，记录免打扰与静音等成员级偏好设置。
/// </summary>
public sealed class Migration007_ConversationMemberPrefs : IRealtimeSchemaMigration
{
    public int Version => 7;
    public string Name => "conversation_member_prefs";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var members = schema.ConversationMembersTableSql;
        var commands = new[]
        {
            $"""
             ALTER TABLE {members}
                 ADD COLUMN IF NOT EXISTS "is_pinned" boolean NOT NULL DEFAULT false;
             """,
            $"""
             ALTER TABLE {members}
                 ADD COLUMN IF NOT EXISTS "pinned_at_ms" bigint NULL;
             """,
            $"""
             ALTER TABLE {members}
                 ADD COLUMN IF NOT EXISTS "is_muted" boolean NOT NULL DEFAULT false;
             """,
            $"""
             ALTER TABLE {members}
                 ADD COLUMN IF NOT EXISTS "muted_until_ms" bigint NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_conversation_members_user_pinned_list"
                 ON {members} (
                     "user_id",
                     "is_pinned" DESC,
                     "pinned_at_ms" DESC NULLS LAST,
                     "conversation_id" DESC
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
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 离群历史策略：软删除成员关系 + 群解散标记。
/// <para>
/// conversation_members 新增 left_at_ms（NULL = 活跃成员；非 NULL = 已离群/被移除，保留只读历史访问）。
/// conversations 新增 dissolved_at_ms（NULL = 正常；非 NULL = 已解散，全员离群但历史保留）。
/// </para>
/// </summary>
public sealed class Migration029_LeaveGroupHistoryPolicy : IRealtimeSchemaMigration
{
    public int Version => 29;
    public string Name => "leave_group_history_policy";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.ConversationMembersTableSql}
                 ADD COLUMN IF NOT EXISTS "left_at_ms" bigint NULL;

             ALTER TABLE {schema.ConversationsTableSql}
                 ADD COLUMN IF NOT EXISTS "dissolved_at_ms" bigint NULL;
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 二-3：归档列表离群快照列。
/// <para>
/// 在 <c>conversation_members</c> 表新增离群时刻的消息快照列，使 <c>QueryArchivedListAsync</c>
/// 能够读取用户离群时看到的最后一条消息，而非群当前 tip。否则群在用户离开后继续活跃时，
/// 归档用户会看到离群后的最新预览和序列。
/// </para>
/// <para>
/// 同时新增 <c>sent_count_at_leave</c>，保留离群时刻的 sent_count，便于审计与回放。
/// 所有列均可空，旧数据保持 NULL，查询时回退到 <c>conversations</c> 表当前值。
/// </para>
/// </summary>
public sealed class Migration037_ArchiveSnapshotColumns : IRealtimeSchemaMigration
{
    public int Version => 37;
    public string Name => "archive_snapshot_columns";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
            ALTER TABLE {schema.ConversationMembersTableSql}
            ADD COLUMN IF NOT EXISTS "left_sequence" bigint NULL,
            ADD COLUMN IF NOT EXISTS "left_message_id" varchar(64) NULL,
            ADD COLUMN IF NOT EXISTS "left_message_preview" text NULL,
            ADD COLUMN IF NOT EXISTS "left_message_at_ms" bigint NULL,
            ADD COLUMN IF NOT EXISTS "left_sender_user_id" bigint NULL,
            ADD COLUMN IF NOT EXISTS "sent_count_at_leave" bigint NULL;
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

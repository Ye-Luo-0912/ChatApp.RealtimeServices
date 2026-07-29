using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// P0-2：为 conversation_members 增加 sent_count_at_retention_floor 列。
/// <para>
/// 当 retention_floor_sequence 晚于成员的 last_read_sequence 时，未读公式的发送基线
/// 不能使用 sent_count_at_read（对应旧 cursor），否则自发送消息会被多扣减。
/// sent_count_at_retention_floor 记录发送者在 retention_floor_sequence 处的累计发送数，
/// 由 Retention 推进 floor 时同步更新。
/// </para>
/// <para>
/// 迁移在事务内执行（RequiresTransaction 默认 true），IF NOT EXISTS 保证可重入。
/// 回填：当前 retention_floor_sequence 默认为 0，last_read_sequence &gt;= 0 恒成立，
/// 故 sent_count_at_retention_floor 初始值 = sent_count_at_read（保守值）。
/// </para>
/// </summary>
public sealed class Migration045_SentCountAtRetentionFloor : IRealtimeSchemaMigration
{
    public int Version => 45;
    public string Name => "sent_count_at_retention_floor";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        // 1. 添加列（NOT NULL DEFAULT 0，幂等可重入）。
        await using var add = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.ConversationMembersTableSql}
             ADD COLUMN IF NOT EXISTS sent_count_at_retention_floor bigint NOT NULL DEFAULT 0;
             """,
            connection,
            transaction);
        await add.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // 2. 回填：初始值 = sent_count_at_read（因为当前 retention_floor_sequence = 0）。
        //    仅回填仍为默认值 0 的行，避免覆盖 Retention 已写入的值。
        await using var backfill = new NpgsqlCommand(
            $"""
             UPDATE {schema.ConversationMembersTableSql}
             SET sent_count_at_retention_floor = sent_count_at_read
             WHERE sent_count_at_retention_floor = 0;
             """,
            connection,
            transaction);
        await backfill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

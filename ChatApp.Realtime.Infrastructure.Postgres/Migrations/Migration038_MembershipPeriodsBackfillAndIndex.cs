using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 二-1：为 <c>conversation_membership_periods</c> 回填历史数据并添加唯一索引。
/// <para>
/// Migration035 创建了表，但历史查询未使用；本迁移：
/// <list type="number">
/// <item>为现有活跃成员（<c>conversation_members.left_at_ms IS NULL</c>）回填初始 period。</item>
/// <item>为已离群成员（<c>conversation_members.left_at_ms IS NOT NULL</c>）回填闭合 period。</item>
/// <item>创建唯一部分索引 <c>ux_membership_periods_open</c>：同一用户在同一会话中最多一个开放 period。</item>
/// </list>
/// </para>
/// <para>
/// 回填使用 <c>ON CONFLICT (conversation_id, user_id, joined_at_ms) DO NOTHING</c>，
/// 重复执行安全；唯一索引使用 <c>IF NOT EXISTS</c>，迁移可重入。
/// </para>
/// </summary>
public sealed class Migration038_MembershipPeriodsBackfillAndIndex : IRealtimeSchemaMigration
{
    public int Version => 38;
    public string Name => "membership_periods_backfill_and_index";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        // 1. 为现有活跃成员回填初始 period（left_at_ms IS NULL 的成员）。
        //    joined_at_ms 从 conversation_members.joined_at_ms 取。
        await using (var backfillActive = new NpgsqlCommand(
            $"""
            INSERT INTO {schema.MembershipPeriodsTableSql} (conversation_id, user_id, joined_at_ms, left_at_ms, left_reason)
            SELECT conversation_id, user_id, joined_at_ms, NULL, NULL
            FROM {schema.ConversationMembersTableSql}
            WHERE left_at_ms IS NULL
            ON CONFLICT (conversation_id, user_id, joined_at_ms) DO NOTHING;
            """,
            connection,
            transaction))
        {
            await backfillActive.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 2. 为已离群成员回填 period（left_at_ms IS NOT NULL 的成员）。
        //    joined_at_ms 从 conversation_members.joined_at_ms 取，left_at_ms 从 conversation_members.left_at_ms 取。
        await using (var backfillLeft = new NpgsqlCommand(
            $"""
            INSERT INTO {schema.MembershipPeriodsTableSql} (conversation_id, user_id, joined_at_ms, left_at_ms, left_reason)
            SELECT conversation_id, user_id, joined_at_ms, left_at_ms, 'leave'
            FROM {schema.ConversationMembersTableSql}
            WHERE left_at_ms IS NOT NULL
            ON CONFLICT (conversation_id, user_id, joined_at_ms) DO NOTHING;
            """,
            connection,
            transaction))
        {
            await backfillLeft.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 3. 唯一部分索引：同一用户在同一会话中最多一个开放 period（left_at_ms IS NULL）。
        //    使用 IF NOT EXISTS 保证迁移可重入。
        await using (var createIndex = new NpgsqlCommand(
            $"""
            CREATE UNIQUE INDEX IF NOT EXISTS "ux_membership_periods_open"
            ON {schema.MembershipPeriodsTableSql} (conversation_id, user_id)
            WHERE left_at_ms IS NULL;
            """,
            connection,
            transaction))
        {
            await createIndex.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 极限-3：群 MarkRead 广播去放大 + debounce 基础列。
/// <para>
/// 新增列：
/// <list type="bullet">
/// <item><c>outbox.exclude_user_id</c>：会话级广播时排除的用户编号。
/// 仅当 <c>audience_kind=1</c>（Conversation）时有效。典型场景：群 MarkRead 广播——
/// 读者本人不需要再收到自己的已读水位通知，通过本字段让 Gateway 在投递时跳过该用户，
/// 无需物化 N-1 个 <c>target_user_ids</c>。</item>
/// <item><c>conversation_members.last_read_broadcast_sequence</c>：该成员最后一次广播
/// ConversationRead 时所推进到的读水位序列。用于 debounce 判定，避免短时间多次 MarkRead
/// 形成 O(N²) 网络投递。</item>
/// <item><c>conversation_members.last_read_broadcast_at_ms</c>：该成员最后一次广播
/// ConversationRead 的服务器时间（毫秒）。配合序列阈值共同决定是否需要再次广播。</item>
/// </list>
/// </para>
/// <para>
/// <b>Debounce 语义</b>：读者每次 MarkRead 都立即写自身 <c>UnreadCountChanged</c>（即时反馈），
/// 仅当满足以下任一条件才向其余成员广播 <c>ConversationRead</c>：
/// <list type="bullet">
/// <item>从未广播过（<c>last_read_broadcast_at_ms IS NULL</c>）；</item>
/// <item>距离上次广播超过 debounce 时间窗；</item>
/// <item>读水位自上次广播推进超过序列阈值。</item>
/// </list>
/// 广播时同事务更新两列水位，未广播则保持原值。
/// </para>
/// </summary>
public sealed class Migration048_ReadBroadcastExclude : IRealtimeSchemaMigration
{
    public int Version => 48;
    public string Name => "read_broadcast_exclude";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;
        var members = schema.ConversationMembersTableSql;

        await using var command = new NpgsqlCommand(
            $"""
            ALTER TABLE {outbox}
                ADD COLUMN IF NOT EXISTS "exclude_user_id" bigint NULL;

            ALTER TABLE {members}
                ADD COLUMN IF NOT EXISTS "last_read_broadcast_sequence" bigint NULL,
                ADD COLUMN IF NOT EXISTS "last_read_broadcast_at_ms" bigint NULL;
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 三-2：为 <c>messages.conversation_sequence</c> 建立会话内唯一约束。
/// <para>
/// Migration032 引入了序列模型并创建了非唯一索引 <c>ix_messages_conversation_sequence</c>，
/// 但未对 <c>(conversation_id, conversation_sequence)</c> 建立唯一约束。本迁移补齐该约束，
/// 作为序列分配逻辑（<c>conversations.last_sequence</c> 原子 +1）的数据库层兜底：
/// 即使并发或代码缺陷导致同会话内出现重复序列号，也会被唯一索引拒绝。
/// </para>
/// <para>
/// 使用部分索引（<c>WHERE conversation_id IS NOT NULL AND conversation_sequence IS NOT NULL</c>）：
/// <list type="bullet">
/// <item>单聊历史消息在 Migration032 回填前可能 <c>conversation_sequence</c> 为 NULL，不参与约束。</item>
/// <item>无会话编号的遗留消息不参与约束。</item>
/// </list>
/// </para>
/// <para>
/// 迁移在事务内执行（<see cref="IRealtimeSchemaMigration.RequiresTransaction"/> 默认 true），
/// 故不使用 <c>CONCURRENTLY</c>；<c>IF NOT EXISTS</c> 保证迁移可重入。
/// </para>
/// </summary>
public sealed class Migration039_UniqueConversationSequence : IRealtimeSchemaMigration
{
    public int Version => 39;
    public string Name => "unique_conversation_sequence";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        // 唯一部分索引：同一会话内消息序列号唯一（仅约束已分配序列号的消息）。
        // 与 Migration032 的非唯一索引 ix_messages_conversation_sequence 共存：
        // - ix_messages_conversation_sequence 覆盖 MarkRead 与历史序列查询（含 NULL 行）。
        // - ux_messages_conversation_sequence 仅约束非 NULL 行，保证写入正确性。
        await using var command = new NpgsqlCommand(
            $"""
            CREATE UNIQUE INDEX IF NOT EXISTS "ux_messages_conversation_sequence"
            ON {schema.MessagesTableSql} (conversation_id, conversation_sequence)
            WHERE conversation_id IS NOT NULL
              AND conversation_sequence IS NOT NULL;
            """,
            connection,
            transaction);

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

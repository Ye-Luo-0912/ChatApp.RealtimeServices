using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 三-3 / 三-4：为 <c>messages</c> 增加 <c>sender_sequence</c>，为 <c>conversations</c>
/// 增加 <c>retention_floor_sequence</c>，并建立 MarkRead O(1) 查询索引。
/// <para>
/// 三-3 背景：Retention 物理删除消息后，列表未读由序列公式计算
/// (<c>last_sequence - last_read_sequence - (sent_count - sent_count_at_read)</c>)，
/// 已删除消息仍可能继续计入未读。新增 <c>retention_floor_sequence</c> 后，
/// 有效读水位为 <c>GREATEST(last_read_sequence, retention_floor_sequence)</c>，
/// 把被删除区间从未读数中扣除。
/// </para>
/// <para>
/// 三-4 背景：<see cref="Stores.ConversationWriteCommands.TryAdvanceReadBySequenceAsync"/>
/// 原本对目标序列前的发送消息执行 <c>COUNT(*)</c> 扫描（O(N)）。新增 <c>sender_sequence</c>
/// 列后，MarkRead 只需 <c>ORDER BY conversation_sequence DESC LIMIT 1</c> 取
/// <c>sender_sequence</c>（O(log N) 索引查找）。
/// </para>
/// <para>
/// 迁移在事务内执行（<see cref="IRealtimeSchemaMigration.RequiresTransaction"/> 默认 true），
/// 故索引不使用 <c>CONCURRENTLY</c>；<c>IF NOT EXISTS</c> 保证迁移可重入。
/// </para>
/// </summary>
public sealed class Migration040_SenderSequenceAndRetentionFloor : IRealtimeSchemaMigration
{
    public int Version => 40;
    public string Name => "sender_sequence_and_retention_floor";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        // 1. messages 表添加 sender_sequence 列（NULL，由写入路径 CTE 回写）。
        await using (var addMessageColumn = new NpgsqlCommand(
                         $"""
                          ALTER TABLE {schema.MessagesTableSql}
                          ADD COLUMN IF NOT EXISTS sender_sequence bigint NULL;
                          """,
                         connection,
                         transaction))
        {
            await addMessageColumn.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 2. conversations 表添加 retention_floor_sequence 列（NOT NULL DEFAULT 0）。
        await using (var addConversationColumn = new NpgsqlCommand(
                         $"""
                          ALTER TABLE {schema.ConversationsTableSql}
                          ADD COLUMN IF NOT EXISTS retention_floor_sequence bigint NOT NULL DEFAULT 0;
                          """,
                         connection,
                         transaction))
        {
            await addConversationColumn.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 3. 回填 sender_sequence：对每个 (conversation_id, sender_user_id) 按 conversation_sequence
        //    排序分配序号。仅回填 conversation_sequence IS NOT NULL 且 sender_sequence IS NULL 的行，
        //    幂等可重入。
        await using (var backfillSenderSequence = new NpgsqlCommand(
                         $"""
                          WITH numbered AS (
                              SELECT message_id,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY conversation_id, sender_user_id
                                         ORDER BY conversation_sequence
                                     )::bigint AS seq
                              FROM {schema.MessagesTableSql}
                              WHERE conversation_sequence IS NOT NULL
                                AND sender_sequence IS NULL
                          )
                          UPDATE {schema.MessagesTableSql} AS m
                          SET sender_sequence = n.seq
                          FROM numbered AS n
                          WHERE m.message_id = n.message_id;
                          """,
                         connection,
                         transaction))
        {
            await backfillSenderSequence.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 4. 索引：MarkRead O(1) 查询用。
        //    (conversation_id, sender_user_id, conversation_sequence DESC) INCLUDE (sender_sequence)
        //    部分索引仅覆盖 conversation_sequence IS NOT NULL 的行，匹配 MarkRead 查询谓词。
        await using (var createIndex = new NpgsqlCommand(
                         $"""
                          CREATE INDEX IF NOT EXISTS "ix_messages_sender_sequence_lookup"
                          ON {schema.MessagesTableSql} (conversation_id, sender_user_id, conversation_sequence DESC)
                          INCLUDE (sender_sequence)
                          WHERE conversation_sequence IS NOT NULL;
                          """,
                         connection,
                         transaction))
        {
            await createIndex.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

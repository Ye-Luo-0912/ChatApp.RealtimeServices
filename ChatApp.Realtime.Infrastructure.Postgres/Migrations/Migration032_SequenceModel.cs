using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Perf-1：序列模型。将群消息热事务从 O(N) 改成 O(1)。
/// <para>
/// 新增列：
/// - conversations.last_sequence：会话当前最大序列号，发消息时原子 +1。
/// - messages.conversation_sequence：消息在会话内的单调递增序列号。
/// - conversation_members.last_read_sequence / sent_count / sent_count_at_read：
///   用于 O(1) 派生未读数，消除 MarkRead 的 COUNT 扫描。
/// </para>
/// <para>
/// 未读数公式：last_sequence - last_read_sequence - (sent_count - sent_count_at_read)
/// - last_sequence - last_read_sequence = 已写入但未读的消息总数
/// - sent_count - sent_count_at_read = 发送者在读水位之后发送的消息数（不应计入未读）
/// </para>
/// <para>
/// 回填采用单事务幂等 UPDATE：只处理 conversation_sequence IS NULL 的行，
/// 重复执行不会覆盖已分配的序列号。生产环境海量数据应使用分批回填（见 Migration009 模式）。
/// </para>
/// </summary>
public sealed class Migration032_SequenceModel : IRealtimeSchemaMigration
{
    public int Version => 32;
    public string Name => "sequence_model";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var conversations = schema.ConversationsTableSql;
        var members = schema.ConversationMembersTableSql;

        // 1. 新增列（幂等）
        await using (var alter = new NpgsqlCommand(
                         $"""
                          ALTER TABLE {conversations}
                              ADD COLUMN IF NOT EXISTS "last_sequence" bigint NOT NULL DEFAULT 0;

                          ALTER TABLE {messages}
                              ADD COLUMN IF NOT EXISTS "conversation_sequence" bigint NULL;

                          ALTER TABLE {members}
                              ADD COLUMN IF NOT EXISTS "last_read_sequence" bigint NULL,
                              ADD COLUMN IF NOT EXISTS "sent_count" bigint NOT NULL DEFAULT 0,
                              ADD COLUMN IF NOT EXISTS "sent_count_at_read" bigint NOT NULL DEFAULT 0;
                          """,
                         connection,
                         transaction))
        {
            await alter.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 2. 为 messages.conversation_sequence 建立索引（MarkRead 与历史序列查询使用）
        await using (var index = new NpgsqlCommand(
                         $"""
                          CREATE INDEX IF NOT EXISTS "ix_messages_conversation_sequence"
                          ON {messages} (conversation_id, conversation_sequence)
                          WHERE conversation_id IS NOT NULL;
                          """,
                         connection,
                         transaction))
        {
            await index.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 3. 回填 messages.conversation_sequence：按 (received_at_ms, message_id) 单调分配
        //    仅处理 NULL 行，幂等可重入。
        await using (var backfillMessages = new NpgsqlCommand(
                         $"""
                          WITH ranked AS (
                              SELECT message_id,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY conversation_id
                                         ORDER BY received_at_ms, message_id
                                     ) AS seq
                              FROM {messages}
                              WHERE conversation_sequence IS NULL
                                AND conversation_id IS NOT NULL
                          )
                          UPDATE {messages} AS m
                          SET conversation_sequence = ranked.seq
                          FROM ranked
                          WHERE m.message_id = ranked.message_id;
                          """,
                         connection,
                         transaction))
        {
            await backfillMessages.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 4. 回填 conversations.last_sequence：取该会话内消息的最大序列号
        await using (var backfillConversations = new NpgsqlCommand(
                         $"""
                          UPDATE {conversations} AS c
                          SET last_sequence = COALESCE(
                              (SELECT MAX(m.conversation_sequence)
                               FROM {messages} AS m
                               WHERE m.conversation_id = c.conversation_id),
                              0
                          )
                          WHERE c.last_sequence = 0;
                          """,
                         connection,
                         transaction))
        {
            await backfillConversations.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 5. 回填 conversation_members.sent_count：用户在该会话内发送的消息数
        await using (var backfillSentCount = new NpgsqlCommand(
                         $"""
                          UPDATE {members} AS m
                          SET sent_count = COALESCE(
                              (SELECT COUNT(*)
                               FROM {messages} AS msg
                               WHERE msg.conversation_id = m.conversation_id
                                 AND msg.sender_user_id = m.user_id
                                 AND msg.conversation_sequence IS NOT NULL),
                              0
                          )
                          WHERE m.sent_count = 0;
                          """,
                         connection,
                         transaction))
        {
            await backfillSentCount.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 6. 回填 conversation_members.last_read_sequence 与 sent_count_at_read
        //    last_read_sequence = last_read_message_id 对应的序列号（NULL/不存在则 0）
        //    sent_count_at_read = 用户在 last_read_sequence 之前发送的消息数
        await using (var backfillMembers = new NpgsqlCommand(
                         $"""
                          WITH read_seq AS (
                              SELECT m.conversation_id, m.user_id,
                                     COALESCE(msg.conversation_sequence, 0) AS read_sequence
                              FROM {members} AS m
                              LEFT JOIN {messages} AS msg
                                  ON msg.message_id = m.last_read_message_id
                              WHERE m.last_read_sequence IS NULL
                          ),
                          sent_at_read AS (
                              SELECT rs.conversation_id, rs.user_id, rs.read_sequence,
                                     (SELECT COUNT(*)
                                      FROM {messages} AS msg2
                                      WHERE msg2.conversation_id = rs.conversation_id
                                        AND msg2.sender_user_id = rs.user_id
                                        AND msg2.conversation_sequence IS NOT NULL
                                        AND msg2.conversation_sequence <= rs.read_sequence) AS sent_upto
                              FROM read_seq rs
                          )
                          UPDATE {members} AS m
                          SET last_read_sequence = sar.read_sequence,
                              sent_count_at_read = sar.sent_upto
                          FROM sent_at_read AS sar
                          WHERE m.conversation_id = sar.conversation_id
                            AND m.user_id = sar.user_id;
                          """,
                         connection,
                         transaction))
        {
            await backfillMembers.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

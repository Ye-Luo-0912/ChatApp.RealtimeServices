using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 会话投影修复：统一 Retention 清理、账号删除、消息批量删除三条路径的 tip 与未读数修复逻辑，
/// 防止语义分叉。
/// <para>
/// 使用 DISTINCT ON 找到受影响会话的剩余最新消息，重新计算 conversation tip 和成员 unread_count。
/// 当会话所有消息都被清理时，tip 置 NULL，unread_count 归零。
/// </para>
/// </summary>
internal static class ConversationProjectionRepair
{
    /// <summary>
    /// 修复会话 tip：用 DISTINCT ON 找到每个受影响会话的剩余最新消息，
    /// 更新 conversations.last_message_* 和 conversation_members.last_message_at_ms。
    /// 所有消息都被清理时，tip 置 NULL。
    /// </summary>
    /// <returns>受影响的 conversations 行数。</returns>
    public static async Task<int> RepairConversationTipsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct)
    {
        if (conversationIds.Count == 0)
            return 0;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var ids = conversationIds.ToArray();

        var repaired = 0;
        await using (var updateConv = new NpgsqlCommand(
                         $"""
                          WITH target AS (
                              SELECT UNNEST(@conversation_ids) AS conversation_id
                          ),
                          latest AS (
                              SELECT DISTINCT ON (conversation_id)
                                  conversation_id, message_id, content, received_at_ms, sender_user_id,
                                  recalled_at_ms IS NOT NULL AS is_recalled
                              FROM {schema.MessagesTableSql}
                              WHERE conversation_id = ANY(@conversation_ids)
                              ORDER BY conversation_id, received_at_ms DESC, message_id DESC
                          ),
                          computed AS (
                              SELECT
                                  t.conversation_id,
                                  l.message_id,
                                  CASE WHEN l.is_recalled THEN '消息已撤回'
                                       WHEN l.content IS NULL THEN NULL
                                       ELSE LEFT(l.content, 256) END AS preview,
                                  l.received_at_ms,
                                  l.sender_user_id
                              FROM target t
                              LEFT JOIN latest l USING (conversation_id)
                          )
                          UPDATE {schema.ConversationsTableSql} AS c
                          SET last_message_id = comp.message_id,
                              last_message_preview = comp.preview,
                              last_message_at_ms = comp.received_at_ms,
                              last_sender_user_id = comp.sender_user_id,
                              updated_at_ms = @now
                          FROM computed comp
                          WHERE c.conversation_id = comp.conversation_id
                            AND (
                                 c.last_message_id IS DISTINCT FROM comp.message_id
                              OR c.last_message_at_ms IS DISTINCT FROM comp.received_at_ms
                              OR c.last_message_preview IS DISTINCT FROM comp.preview
                              OR c.last_sender_user_id IS DISTINCT FROM comp.sender_user_id
                            );
                          """,
                         connection,
                         transaction))
        {
            updateConv.Parameters.AddWithValue("conversation_ids", ids);
            updateConv.Parameters.AddWithValue("now", now);
            repaired = await updateConv.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var updateMembers = new NpgsqlCommand(
                         $"""
                          WITH target AS (
                              SELECT UNNEST(@conversation_ids) AS conversation_id
                          ),
                          latest AS (
                              SELECT DISTINCT ON (conversation_id)
                                  conversation_id, received_at_ms
                              FROM {schema.MessagesTableSql}
                              WHERE conversation_id = ANY(@conversation_ids)
                              ORDER BY conversation_id, received_at_ms DESC, message_id DESC
                          ),
                          computed AS (
                              SELECT t.conversation_id, l.received_at_ms
                              FROM target t
                              LEFT JOIN latest l USING (conversation_id)
                          )
                          UPDATE {schema.ConversationMembersTableSql} AS m
                          SET last_message_at_ms = comp.received_at_ms
                          FROM computed comp
                          WHERE m.conversation_id = comp.conversation_id
                            AND m.last_message_at_ms IS DISTINCT FROM comp.received_at_ms;
                          """,
                         connection,
                         transaction))
        {
            updateMembers.Parameters.AddWithValue("conversation_ids", ids);
            await updateMembers.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        return repaired;
    }

    /// <summary>
    /// 修复成员未读数：根据剩余消息和成员的 last_read 水位重新计算 unread_count，
    /// 而非粗暴归零。保留成员的 last_read_message_id 和 last_read_at_ms 不变。
    /// </summary>
    /// <returns>受影响的 conversation_members 行数。</returns>
    public static async Task<int> RepairUnreadCountsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct)
    {
        if (conversationIds.Count == 0)
            return 0;

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {schema.ConversationMembersTableSql} AS m
             SET unread_count = repaired.new_unread
             FROM (
                 SELECT
                     cm.conversation_id,
                     cm.user_id,
                     LEAST(
                         (
                             SELECT COUNT(*)::int
                             FROM (
                                 SELECT 1
                                 FROM {schema.MessagesTableSql} msg
                                 WHERE msg.conversation_id = cm.conversation_id
                                   AND msg.sender_user_id <> cm.user_id
                                   AND (
                                        cm.last_read_at_ms IS NULL
                                        OR msg.received_at_ms > cm.last_read_at_ms
                                        OR (
                                            msg.received_at_ms = cm.last_read_at_ms
                                            AND (
                                                cm.last_read_message_id IS NULL
                                                OR msg.message_id > cm.last_read_message_id
                                            )
                                        )
                                   )
                                 LIMIT @max_unread
                             ) AS bounded
                         ),
                         @max_unread
                     ) AS new_unread
                 FROM {schema.ConversationMembersTableSql} cm
                 WHERE cm.conversation_id = ANY(@conversation_ids)
             ) AS repaired
             WHERE m.conversation_id = repaired.conversation_id
               AND m.user_id = repaired.user_id
               AND m.unread_count IS DISTINCT FROM repaired.new_unread;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_ids", conversationIds.ToArray());
        command.Parameters.AddWithValue("max_unread", ConversationWriteCommands.MaxTrackedUnreadCount);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

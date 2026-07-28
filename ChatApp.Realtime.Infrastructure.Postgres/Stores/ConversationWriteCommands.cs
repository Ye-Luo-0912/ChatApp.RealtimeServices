using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 会话投影写入：尽量少往返，条件更新保证乱序不回退。
/// </summary>
internal static class ConversationWriteCommands
{
    public const int MaxTrackedUnreadCount = 10_000;

    /// <summary>
    /// 会话 tip UPSERT + 成员确保 + member tip，与接收方未读递增合并为一次 NpgsqlBatch（两语句、一往返）。
    /// 未读必须与 ensure_members 分开：PG modifying CTE 共享快照，看不到同 WITH 内新插入的成员行。
    /// </summary>
    public static async Task<(bool Advanced, int? UnreadCount)> TryAdvanceAndIncrementUnreadAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string messageId,
        string preview,
        long receivedAtMs,
        CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(connection, transaction);

        var advance = new NpgsqlBatchCommand(
            $"""
             WITH upsert_conversation AS (
                 INSERT INTO {schema.ConversationsTableSql} (
                     conversation_id, type, created_at_ms, updated_at_ms,
                     last_message_id, last_message_preview, last_message_at_ms, last_sender_user_id
                 ) VALUES (
                     @conversation_id, @type, @received_at_ms, @received_at_ms,
                     @message_id, @preview, @received_at_ms, @sender_user_id
                 )
                 ON CONFLICT (conversation_id) DO UPDATE SET
                     last_message_id = EXCLUDED.last_message_id,
                     last_message_preview = EXCLUDED.last_message_preview,
                     last_message_at_ms = EXCLUDED.last_message_at_ms,
                     last_sender_user_id = EXCLUDED.last_sender_user_id,
                     updated_at_ms = EXCLUDED.updated_at_ms
                 WHERE {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                    OR ({schema.ConversationsTableSql}.last_message_at_ms, {schema.ConversationsTableSql}.last_message_id)
                       < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                 RETURNING conversation_id
             ),
             ensure_members AS (
                 INSERT INTO {schema.ConversationMembersTableSql} (
                     conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms
                 ) VALUES
                     (@conversation_id, @sender_user_id, @receiver_user_id, @received_at_ms, @received_at_ms),
                     (@conversation_id, @receiver_user_id, @sender_user_id, @received_at_ms, @received_at_ms)
                 ON CONFLICT (conversation_id, user_id) DO NOTHING
                 RETURNING user_id
             ),
             members_ready AS (
                 SELECT COALESCE((SELECT COUNT(*)::int FROM ensure_members), 0) AS ensured
             ),
             sync_tip AS (
                 UPDATE {schema.ConversationMembersTableSql}
                 SET last_message_at_ms = @received_at_ms
                 WHERE conversation_id = @conversation_id
                   AND EXISTS (SELECT 1 FROM upsert_conversation)
                   AND (SELECT ensured FROM members_ready) >= 0
                   AND (
                        last_message_at_ms IS NULL
                        OR last_message_at_ms < @received_at_ms
                   )
                 RETURNING user_id
             )
             SELECT EXISTS (SELECT 1 FROM upsert_conversation) AS advanced;
             """);
        advance.Parameters.AddWithValue("conversation_id", conversationId);
        advance.Parameters.AddWithValue("type", (short)ConversationType.Direct);
        advance.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        advance.Parameters.AddWithValue("message_id", messageId);
        advance.Parameters.AddWithValue("preview", preview);
        advance.Parameters.AddWithValue("sender_user_id", senderUserId);
        advance.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        batch.BatchCommands.Add(advance);

        var unreadCmd = new NpgsqlBatchCommand(
            $"""
             UPDATE {schema.ConversationMembersTableSql}
             SET unread_count = LEAST(unread_count + 1, @max_unread)
             WHERE conversation_id = @conversation_id
               AND user_id = @receiver_user_id
               AND (
                    last_read_at_ms IS NULL
                    OR last_read_at_ms < @received_at_ms
                    OR (last_read_at_ms = @received_at_ms
                        AND (last_read_message_id IS NULL OR last_read_message_id < @message_id))
               )
             RETURNING unread_count;
             """);
        unreadCmd.Parameters.AddWithValue("max_unread", MaxTrackedUnreadCount);
        unreadCmd.Parameters.AddWithValue("conversation_id", conversationId);
        unreadCmd.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        unreadCmd.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        unreadCmd.Parameters.AddWithValue("message_id", messageId);
        batch.BatchCommands.Add(unreadCmd);

        await using var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);

        var advanced = false;
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
            advanced = reader.GetBoolean(0);

        int? unread = null;
        if (await reader.NextResultAsync(ct).ConfigureAwait(false)
            && await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            unread = reader.GetInt32(0);
        }

        return (advanced, unread);
    }

    /// <summary>
    /// 群 tip 前进 + 全员 tip 同步 + 非发送方未读递增。不创建成员（成员须已存在）。
    /// </summary>
    public static async Task<(bool Advanced, IReadOnlyList<(long UserId, int UnreadCount)> Unreads)>
        TryAdvanceGroupAndIncrementUnreadAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            RealtimeDatabaseSchema schema,
            string conversationId,
            long senderUserId,
            string messageId,
            string preview,
            long receivedAtMs,
            CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(connection, transaction);

        var advance = new NpgsqlBatchCommand(
            $"""
             WITH upsert_conversation AS (
                 UPDATE {schema.ConversationsTableSql}
                 SET last_message_id = @message_id,
                     last_message_preview = @preview,
                     last_message_at_ms = @received_at_ms,
                     last_sender_user_id = @sender_user_id,
                     updated_at_ms = @received_at_ms
                 WHERE conversation_id = @conversation_id
                   AND type = @group_type
                   AND (
                        last_message_at_ms IS NULL
                        OR (last_message_at_ms, last_message_id)
                           < (@received_at_ms, @message_id)
                   )
                 RETURNING conversation_id
             ),
             sync_tip AS (
                 UPDATE {schema.ConversationMembersTableSql}
                 SET last_message_at_ms = @received_at_ms
                 WHERE conversation_id = @conversation_id
                   AND EXISTS (SELECT 1 FROM upsert_conversation)
                   AND (
                        last_message_at_ms IS NULL
                        OR last_message_at_ms < @received_at_ms
                   )
                 RETURNING user_id
             )
             SELECT EXISTS (SELECT 1 FROM upsert_conversation) AS advanced;
             """);
        advance.Parameters.AddWithValue("conversation_id", conversationId);
        advance.Parameters.AddWithValue("group_type", (short)ConversationType.Group);
        advance.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        advance.Parameters.AddWithValue("message_id", messageId);
        advance.Parameters.AddWithValue("preview", preview);
        advance.Parameters.AddWithValue("sender_user_id", senderUserId);
        batch.BatchCommands.Add(advance);

        var unreadCmd = new NpgsqlBatchCommand(
            $"""
             UPDATE {schema.ConversationMembersTableSql}
             SET unread_count = LEAST(unread_count + 1, @max_unread)
             WHERE conversation_id = @conversation_id
               AND user_id <> @sender_user_id
               AND (
                    last_read_at_ms IS NULL
                    OR last_read_at_ms < @received_at_ms
                    OR (last_read_at_ms = @received_at_ms
                        AND (last_read_message_id IS NULL OR last_read_message_id < @message_id))
               )
             RETURNING user_id, unread_count;
             """);
        unreadCmd.Parameters.AddWithValue("max_unread", MaxTrackedUnreadCount);
        unreadCmd.Parameters.AddWithValue("conversation_id", conversationId);
        unreadCmd.Parameters.AddWithValue("sender_user_id", senderUserId);
        unreadCmd.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        unreadCmd.Parameters.AddWithValue("message_id", messageId);
        batch.BatchCommands.Add(unreadCmd);

        await using var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);

        var advanced = false;
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
            advanced = reader.GetBoolean(0);

        var unreads = new List<(long UserId, int UnreadCount)>();
        if (await reader.NextResultAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                unreads.Add((reader.GetInt64(0), reader.GetInt32(1)));
        }

        return (advanced, unreads);
    }

    public static async Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT user_id
             FROM {schema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
               AND left_at_ms IS NULL
             ORDER BY user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    public static async Task<bool> TryAdvanceDirectConversationAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string messageId,
        string preview,
        long receivedAtMs,
        CancellationToken ct)
    {
        // 单条 UPSERT：插入时带摘要；冲突时仅当新消息复合序更大才前进。
        await using (var upsertConversation = new NpgsqlCommand(
                           $"""
                            INSERT INTO {schema.ConversationsTableSql} (
                                conversation_id, type, created_at_ms, updated_at_ms,
                                last_message_id, last_message_preview, last_message_at_ms, last_sender_user_id
                            ) VALUES (
                                @conversation_id, @type, @received_at_ms, @received_at_ms,
                                @message_id, @preview, @received_at_ms, @sender_user_id
                            )
                            ON CONFLICT (conversation_id) DO UPDATE SET
                                last_message_id = EXCLUDED.last_message_id,
                                last_message_preview = EXCLUDED.last_message_preview,
                                last_message_at_ms = EXCLUDED.last_message_at_ms,
                                last_sender_user_id = EXCLUDED.last_sender_user_id,
                                updated_at_ms = EXCLUDED.updated_at_ms
                            WHERE {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                               OR ({schema.ConversationsTableSql}.last_message_at_ms, {schema.ConversationsTableSql}.last_message_id)
                                  < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                            RETURNING conversation_id;
                            """,
                           connection,
                           transaction))
        {
            upsertConversation.Parameters.AddWithValue("conversation_id", conversationId);
            upsertConversation.Parameters.AddWithValue("type", (short)ConversationType.Direct);
            upsertConversation.Parameters.AddWithValue("received_at_ms", receivedAtMs);
            upsertConversation.Parameters.AddWithValue("message_id", messageId);
            upsertConversation.Parameters.AddWithValue("preview", preview);
            upsertConversation.Parameters.AddWithValue("sender_user_id", senderUserId);

            var advanced = await upsertConversation.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (advanced is null)
            {
                // 摘要未前进，仍确保成员存在（幂等）。
                await EnsureMembersAsync(
                        connection,
                        transaction,
                        schema,
                        conversationId,
                        senderUserId,
                        receiverUserId,
                        receivedAtMs,
                        ct)
                    .ConfigureAwait(false);
                return false;
            }
        }

        await EnsureMembersAsync(
                connection,
                transaction,
                schema,
                conversationId,
                senderUserId,
                receiverUserId,
                receivedAtMs,
                ct)
            .ConfigureAwait(false);

        await using (var syncMemberTip = new NpgsqlCommand(
                           $"""
                            UPDATE {schema.ConversationMembersTableSql}
                            SET last_message_at_ms = @received_at_ms
                            WHERE conversation_id = @conversation_id
                              AND (
                                   last_message_at_ms IS NULL
                                   OR last_message_at_ms < @received_at_ms
                              );
                            """,
                           connection,
                           transaction))
        {
            syncMemberTip.Parameters.AddWithValue("conversation_id", conversationId);
            syncMemberTip.Parameters.AddWithValue("received_at_ms", receivedAtMs);
            await syncMemberTip.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// 接收方未读 +1（有界）；仅当新消息严格晚于其已读游标时生效。
    /// </summary>
    public static async Task<int?> TryIncrementReceiverUnreadAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long receiverUserId,
        string messageId,
        long receivedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {schema.ConversationMembersTableSql}
             SET unread_count = LEAST(unread_count + 1, @max_unread)
             WHERE conversation_id = @conversation_id
               AND user_id = @receiver_user_id
               AND (
                    last_read_at_ms IS NULL
                    OR last_read_at_ms < @received_at_ms
                    OR (last_read_at_ms = @received_at_ms
                        AND (last_read_message_id IS NULL OR last_read_message_id < @message_id))
               )
             RETURNING unread_count;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("max_unread", MaxTrackedUnreadCount);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        command.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        command.Parameters.AddWithValue("message_id", messageId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int unread ? unread : result is long unread64 ? (int)unread64 : null;
    }

    public static RealtimeEvent CreateUnreadCountChangedEvent(
        string conversationId,
        long targetUserId,
        int unreadCount,
        string? lastReadMessageId,
        long? lastReadAtMs,
        string causeMessageId,
        long occurredAtMs,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeUnreadCountChangedPayload
        {
            ConversationId = conversationId,
            UnreadCount = unreadCount,
            LastReadMessageId = lastReadMessageId,
            LastReadAtMs = lastReadAtMs
        };

        return new RealtimeEvent
        {
            EventId = ConversationEventIdFactory.CreateUnreadCountChangedEventId(
                conversationId,
                targetUserId,
                unreadCount,
                lastReadMessageId,
                lastReadAtMs,
                causeMessageId),
            Type = RealtimeEventType.UnreadCountChanged,
            TargetUserId = targetUserId,
            MessageId = causeMessageId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeUnreadCountChangedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    public static string CreateUnreadCountChangedEventId(
        string conversationId,
        long targetUserId,
        int unreadCount,
        string? lastReadMessageId,
        long? lastReadAtMs,
        string causeMessageId) =>
        ConversationEventIdFactory.CreateUnreadCountChangedEventId(
            conversationId,
            targetUserId,
            unreadCount,
            lastReadMessageId,
            lastReadAtMs,
            causeMessageId);

    public static RealtimeEvent CreateConversationReadEvent(
        string conversationId,
        long targetUserId,
        long readerUserId,
        string lastReadMessageId,
        long lastReadAtMs,
        long occurredAtMs,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeConversationReadPayload
        {
            ConversationId = conversationId,
            ReaderUserId = readerUserId,
            LastReadMessageId = lastReadMessageId,
            LastReadAtMs = lastReadAtMs
        };

        return new RealtimeEvent
        {
            EventId = ConversationEventIdFactory.CreateConversationReadEventId(
                conversationId,
                readerUserId,
                lastReadMessageId,
                lastReadAtMs,
                targetUserId),
            Type = RealtimeEventType.ConversationRead,
            TargetUserId = targetUserId,
            ActorUserId = readerUserId,
            MessageId = lastReadMessageId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationReadPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    public static RealtimeEvent CreateConversationChangedEvent(
        string conversationId,
        long targetUserId,
        long? peerUserId,
        string messageId,
        string preview,
        long receivedAtMs,
        long senderUserId,
        string? traceParent,
        string? traceState,
        string? eventIdCause = null,
        ConversationType type = ConversationType.Direct,
        string? title = null,
        long? lastSequence = null)
    {
        var payload = new RealtimeConversationChangedPayload
        {
            ConversationId = conversationId,
            Type = type,
            PeerUserId = peerUserId,
            Title = title,
            LastMessageId = messageId,
            LastMessagePreview = preview,
            LastMessageAtMs = receivedAtMs,
            LastSenderUserId = senderUserId,
            LastSequence = lastSequence
        };

        return new RealtimeEvent
        {
            EventId = CreateConversationChangedEventId(
                conversationId,
                messageId,
                targetUserId,
                eventIdCause),
            Type = RealtimeEventType.ConversationListChanged,
            TargetUserId = targetUserId,
            ActorUserId = senderUserId,
            MessageId = messageId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationChangedPayload),
            OccurredAtMs = receivedAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    public static RealtimeEvent CreateConversationPrefsChangedEvent(
        string conversationId,
        long targetUserId,
        ConversationType type,
        long? peerUserId,
        string? lastMessageId,
        string? lastMessagePreview,
        long? lastMessageAtMs,
        long? lastSenderUserId,
        bool isPinned,
        long? pinnedAtMs,
        bool isMuted,
        long? mutedUntilMs,
        long occurredAtMs,
        string? traceParent,
        string? traceState,
        long? lastSequence = null)
    {
        var payload = new RealtimeConversationChangedPayload
        {
            ConversationId = conversationId,
            Type = type,
            PeerUserId = peerUserId,
            LastMessageId = lastMessageId,
            LastMessagePreview = lastMessagePreview,
            LastMessageAtMs = lastMessageAtMs,
            LastSenderUserId = lastSenderUserId,
            IsPinned = isPinned,
            IsMuted = isMuted,
            MutedUntilMs = mutedUntilMs,
            LastSequence = lastSequence
        };

        return new RealtimeEvent
        {
            EventId = CreateConversationPrefsChangedEventId(
                conversationId,
                targetUserId,
                isPinned,
                pinnedAtMs,
                isMuted,
                mutedUntilMs),
            Type = RealtimeEventType.ConversationListChanged,
            TargetUserId = targetUserId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationChangedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    public static string CreateConversationChangedEventId(
        string conversationId,
        string messageId,
        long targetUserId,
        string? causeToken = null) =>
        ConversationEventIdFactory.CreateConversationChangedEventId(
            conversationId,
            messageId,
            targetUserId,
            causeToken);

    public static string CreateConversationPrefsChangedEventId(
        string conversationId,
        long targetUserId,
        bool isPinned,
        long? pinnedAtMs,
        bool isMuted,
        long? mutedUntilMs) =>
        ConversationEventIdFactory.CreateConversationPrefsChangedEventId(
            conversationId,
            targetUserId,
            isPinned,
            pinnedAtMs,
            isMuted,
            mutedUntilMs);

    private static async Task EnsureMembersAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        long joinedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.ConversationMembersTableSql} (
                 conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms
             ) VALUES
                 (@conversation_id, @sender_user_id, @receiver_user_id, @joined_at_ms, @joined_at_ms),
                 (@conversation_id, @receiver_user_id, @sender_user_id, @joined_at_ms, @joined_at_ms)
             ON CONFLICT (conversation_id, user_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Perf-1：群消息序列推进 O(1)。原子递增 conversations.last_sequence 并回写 messages.conversation_sequence，
    /// 仅递增发送者的 sent_count，不再触碰其他成员行。
    /// 返回新序列号；若会话不存在或非群则返回 null。
    /// </summary>
    public static async Task<long?> TryAdvanceGroupSequenceAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long senderUserId,
        string messageId,
        string preview,
        long receivedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
            WITH advanced AS (
                UPDATE {schema.ConversationsTableSql}
                SET last_sequence = last_sequence + 1,
                    last_message_id = @message_id,
                    last_message_preview = @preview,
                    last_message_at_ms = @received_at_ms,
                    last_sender_user_id = @sender_user_id,
                    updated_at_ms = @received_at_ms
                WHERE conversation_id = @conversation_id
                  AND type = @group_type
                RETURNING last_sequence
            ),
            sender_bump AS (
                UPDATE {schema.ConversationMembersTableSql}
                SET sent_count = sent_count + 1
                WHERE conversation_id = @conversation_id
                  AND user_id = @sender_user_id
                  AND left_at_ms IS NULL
                  AND EXISTS (SELECT 1 FROM advanced)
            ),
            msg_seq AS (
                UPDATE {schema.MessagesTableSql}
                SET conversation_sequence = (SELECT last_sequence FROM advanced)
                WHERE message_id = @message_id
                  AND conversation_id = @conversation_id
                  AND EXISTS (SELECT 1 FROM advanced)
            )
            SELECT last_sequence FROM advanced;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("group_type", (short)ConversationType.Group);
        command.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("preview", preview);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long seq ? seq : (result is int i ? (long)i : null);
    }

    /// <summary>
    /// Perf-1：单聊消息序列推进 O(1)。UPSERT 会话行 + 递增 last_sequence + 回写 message 序列 + 发送者 sent_count。
    /// 接收方 sent_count 不递增（接收方未发送）。同时基于序列公式派生并写回接收方 unread_count，
    /// 使单聊仍可发出 UnreadCountChanged 事件（单聊接收方仅 1 人，O(1)）。
    /// 序列号始终递增（保证每条消息获得单调序列），但 tip（last_message_id/at_ms）仅在消息更新时前进，
    /// 支持乱序消息不回退 tip。
    /// 返回新序列号与接收方未读数。
    /// </summary>
    public static async Task<(long? Sequence, int? ReceiverUnread)> TryAdvanceDirectSequenceAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string messageId,
        string preview,
        long receivedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
            WITH upsert_conversation AS (
                INSERT INTO {schema.ConversationsTableSql} (
                    conversation_id, type, created_at_ms, updated_at_ms,
                    last_message_id, last_message_preview, last_message_at_ms,
                    last_sender_user_id, last_sequence
                ) VALUES (
                    @conversation_id, @type, @received_at_ms, @received_at_ms,
                    @message_id, @preview, @received_at_ms,
                    @sender_user_id, 1
                )
                ON CONFLICT (conversation_id) DO UPDATE SET
                    -- 序列始终递增（保证每条消息获得单调序列）
                    last_sequence = {schema.ConversationsTableSql}.last_sequence + 1,
                    -- tip 字段仅在消息时间更新时前进，支持乱序消息不回退
                    last_message_id = CASE
                        WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                             OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                 {schema.ConversationsTableSql}.last_message_id)
                                < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                        THEN EXCLUDED.last_message_id
                        ELSE {schema.ConversationsTableSql}.last_message_id
                    END,
                    last_message_preview = CASE
                        WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                             OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                 {schema.ConversationsTableSql}.last_message_id)
                                < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                        THEN EXCLUDED.last_message_preview
                        ELSE {schema.ConversationsTableSql}.last_message_preview
                    END,
                    last_message_at_ms = CASE
                        WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                             OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                 {schema.ConversationsTableSql}.last_message_id)
                                < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                        THEN EXCLUDED.last_message_at_ms
                        ELSE {schema.ConversationsTableSql}.last_message_at_ms
                    END,
                    last_sender_user_id = CASE
                        WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                             OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                 {schema.ConversationsTableSql}.last_message_id)
                                < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                        THEN EXCLUDED.last_sender_user_id
                        ELSE {schema.ConversationsTableSql}.last_sender_user_id
                    END,
                    updated_at_ms = EXCLUDED.updated_at_ms
                RETURNING last_sequence
            ),
            ensure_receiver AS (
                INSERT INTO {schema.ConversationMembersTableSql} (
                    conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms
                ) VALUES
                    (@conversation_id, @receiver_user_id, @sender_user_id, @received_at_ms, @received_at_ms)
                ON CONFLICT (conversation_id, user_id) DO NOTHING
            ),
            sender_upsert AS (
                -- 发送方行不存在时插入 sent_count=1；已存在时递增 sent_count。
                -- 合并 ensure + bump 避免同语句内 INSERT 与 UPDATE 互相不可见的问题。
                INSERT INTO {schema.ConversationMembersTableSql} (
                    conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms, sent_count
                ) VALUES
                    (@conversation_id, @sender_user_id, @receiver_user_id, @received_at_ms, @received_at_ms, 1)
                ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                    sent_count = {schema.ConversationMembersTableSql}.sent_count + 1,
                    last_message_at_ms = @received_at_ms
                WHERE EXISTS (SELECT 1 FROM upsert_conversation)
            ),
            msg_seq AS (
                UPDATE {schema.MessagesTableSql}
                SET conversation_sequence = (SELECT last_sequence FROM upsert_conversation)
                WHERE message_id = @message_id
                  AND EXISTS (SELECT 1 FROM upsert_conversation)
            ),
            receiver_unread AS (
                -- 仅对已存在的接收方行生效（新行由 ensure_receiver 刚插入，同语句不可见）。
                -- 新行的 unread 通过最终 SELECT 的 COALESCE fallback 计算。
                UPDATE {schema.ConversationMembersTableSql}
                SET unread_count = LEAST(
                    GREATEST(
                        (SELECT last_sequence FROM upsert_conversation)
                        - COALESCE(last_read_sequence, 0)
                        - (sent_count - sent_count_at_read),
                        0
                    ),
                    @max_unread
                )
                WHERE conversation_id = @conversation_id
                  AND user_id = @receiver_user_id
                  AND EXISTS (SELECT 1 FROM upsert_conversation)
                RETURNING unread_count
            )
            SELECT
                (SELECT last_sequence FROM upsert_conversation),
                -- COALESCE fallback：新接收方行未被 UPDATE 命中时，
                -- 未读 = last_sequence（新成员 last_read_sequence=0, sent_count=0, sent_count_at_read=0）
                LEAST(
                    COALESCE(
                        (SELECT unread_count FROM receiver_unread),
                        (SELECT last_sequence FROM upsert_conversation)
                    ),
                    @max_unread
                );
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("type", (short)ConversationType.Direct);
        command.Parameters.AddWithValue("received_at_ms", receivedAtMs);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("preview", preview);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        command.Parameters.AddWithValue("max_unread", MaxTrackedUnreadCount);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return (null, null);

        var seqObj = reader.GetValue(0);
        var sequence = seqObj is long s ? s : (seqObj is int si ? (long)si : (long?)null);
        int? receiverUnread = reader.IsDBNull(1) ? null : reader.GetInt32(1);
        return (sequence, receiverUnread);
    }

    /// <summary>
    /// Perf-1：基于序列的 O(1) MarkRead。消除 COUNT 扫描，使用索引查询 + 单行 UPDATE。
    /// <para>
    /// unread = last_sequence - target_sequence - (sent_count - sent_upto_target)
    /// 其中 sent_upto_target = 用户在 target_sequence 之前发送的消息数（索引查询 O(log N)）。
    /// </para>
    /// </summary>
    public static async Task<SequenceReadAdvanceResult> TryAdvanceReadBySequenceAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        long userId,
        long targetSequence,
        string targetMessageId,
        long targetReceivedAtMs,
        CancellationToken ct)
    {
        // 读取会话 last_sequence 与成员当前 sent_count / last_read_sequence
        long conversationLastSequence;
        long memberSentCount;
        long? currentReadSequence;

        await using (var load = new NpgsqlCommand(
                         $"""
                          SELECT c.last_sequence,
                                 m.sent_count,
                                 m.last_read_sequence
                          FROM {schema.ConversationsTableSql} AS c
                          INNER JOIN {schema.ConversationMembersTableSql} AS m
                              ON m.conversation_id = c.conversation_id
                          WHERE c.conversation_id = @conversation_id
                            AND m.user_id = @user_id
                          FOR UPDATE OF m;
                          """,
                         connection,
                         transaction))
        {
            load.Parameters.AddWithValue("conversation_id", conversationId);
            load.Parameters.AddWithValue("user_id", userId);
            await using var reader = await load.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return SequenceReadAdvanceResult.NotMember;
            conversationLastSequence = reader.GetInt64(0);
            memberSentCount = reader.GetInt64(1);
            currentReadSequence = reader.IsDBNull(2) ? null : reader.GetInt64(2);
        }

        // 已读到目标或更晚水位：无需推进
        if (currentReadSequence is long crs && crs >= targetSequence)
        {
            var currentUnread = conversationLastSequence - crs;
            return new SequenceReadAdvanceResult(
                Advanced: false,
                UnreadCount: Math.Max(0, (int)currentUnread),
                LastSequence: conversationLastSequence);
        }

        // 索引查询：用户在 target_sequence 之前（含）发送的消息数
        int sentUptoTarget;
        await using (var sentCount = new NpgsqlCommand(
                         $"""
                          SELECT COUNT(*)::int
                          FROM {schema.MessagesTableSql}
                          WHERE conversation_id = @conversation_id
                            AND sender_user_id = @user_id
                            AND conversation_sequence IS NOT NULL
                            AND conversation_sequence <= @target_sequence;
                          """,
                         connection,
                         transaction))
        {
            sentCount.Parameters.AddWithValue("conversation_id", conversationId);
            sentCount.Parameters.AddWithValue("user_id", userId);
            sentCount.Parameters.AddWithValue("target_sequence", targetSequence);
            var sentObj = await sentCount.ExecuteScalarAsync(ct).ConfigureAwait(false);
            sentUptoTarget = sentObj is int v ? v : Convert.ToInt32(sentObj);
        }

        // unread = last_sequence - target - (sent_count - sent_upto_target)
        var unread = conversationLastSequence - targetSequence - (memberSentCount - sentUptoTarget);
        var unreadInt = Math.Max(0, (int)Math.Min(unread, MaxTrackedUnreadCount));

        await using (var update = new NpgsqlCommand(
                         $"""
                          UPDATE {schema.ConversationMembersTableSql}
                          SET last_read_sequence = @target_sequence,
                              sent_count_at_read = @sent_upto_target,
                              last_read_at_ms = @read_at_ms,
                              last_read_message_id = @read_message_id,
                              unread_count = @unread_count
                          WHERE conversation_id = @conversation_id
                            AND user_id = @user_id;
                          """,
                         connection,
                         transaction))
        {
            update.Parameters.AddWithValue("target_sequence", targetSequence);
            update.Parameters.AddWithValue("sent_upto_target", (long)sentUptoTarget);
            update.Parameters.AddWithValue("read_at_ms", targetReceivedAtMs);
            update.Parameters.AddWithValue("read_message_id", targetMessageId);
            update.Parameters.AddWithValue("unread_count", unreadInt);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        return new SequenceReadAdvanceResult(
            Advanced: true,
            UnreadCount: unreadInt,
            LastSequence: conversationLastSequence);
    }
}

/// <summary>
/// Perf-1：序列化 MarkRead 结果。
/// </summary>
internal sealed record SequenceReadAdvanceResult(
    bool Advanced,
    int UnreadCount,
    long LastSequence)
{
    public static SequenceReadAdvanceResult NotMember { get; } =
        new(false, 0, 0);
}

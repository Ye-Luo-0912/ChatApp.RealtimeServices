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
            EventId = RealtimeEventContracts.CreateUnreadCountChangedEventId(
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
        RealtimeEventContracts.CreateUnreadCountChangedEventId(
            conversationId,
            targetUserId,
            unreadCount,
            lastReadMessageId,
            lastReadAtMs,
            causeMessageId);

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
        string? title = null)
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
            LastSenderUserId = senderUserId
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
        string? traceState)
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
            MutedUntilMs = mutedUntilMs
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
        RealtimeEventContracts.CreateConversationChangedEventId(
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
        RealtimeEventContracts.CreateConversationPrefsChangedEventId(
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
}

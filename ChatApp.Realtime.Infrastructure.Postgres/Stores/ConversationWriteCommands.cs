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
    /// P0-2：统一的未读数 SQL 表达式。基于序列公式派生，处理 retention floor 与自发送消息扣除。
    /// <para>
    /// 有效读水位 = GREATEST(last_read_sequence, retention_floor_sequence)，
    /// 把已被 Retention 物理删除的区间从未读数中扣除。
    /// 当 last_read_sequence &gt;= retention_floor_sequence 时，使用 sent_count_at_read 作为发送基线；
    /// 否则使用 sent_count_at_retention_floor（Retention 推进 floor 时同步更新）。
    /// </para>
    /// <para>
    /// 要求查询使用 c (conversations) 与 m (conversation_members) 别名。
    /// </para>
    /// </summary>
    public static readonly string UnreadCountSqlExpression = $"""
        LEAST(
            GREATEST(
                COALESCE(c.last_sequence, 0)
                - GREATEST(COALESCE(m.last_read_sequence, 0), COALESCE(c.retention_floor_sequence, 0))
                - (m.sent_count - CASE
                    WHEN COALESCE(m.last_read_sequence, 0) >= COALESCE(c.retention_floor_sequence, 0)
                    THEN m.sent_count_at_read
                    ELSE m.sent_count_at_retention_floor
                  END),
                0
            ),
            {MaxTrackedUnreadCount}
        )::int
        """;

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
        long? lastReadSequence,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeConversationReadPayload
        {
            ConversationId = conversationId,
            ReaderUserId = readerUserId,
            LastReadMessageId = lastReadMessageId,
            LastReadAtMs = lastReadAtMs,
            LastReadSequence = lastReadSequence
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

    /// <summary>
    /// Perf-1：群消息序列推进 O(1)。原子递增 conversations.last_sequence 并回写 messages.conversation_sequence
    /// 与 messages.sender_sequence，仅递增发送者的 sent_count，不再触碰其他成员行。
    /// <para>
    /// P0-3：授权检查嵌入 UPDATE 谓词，消除"先查成员 → 再推进序列"的 TOCTOU 竞态。
    /// 仅当会话存在、类型为群、未解散且发送者仍是活跃成员（left_at_ms IS NULL）时才推进。
    /// 否则 <c>advanced</c> CTE 返回 0 行，下游 <c>sender_bump</c> / <c>msg_seq</c> 因
    /// <c>EXISTS (SELECT 1 FROM advanced)</c> 为 false 而不执行，<c>SELECT last_sequence FROM advanced</c>
    /// 返回 null，调用方据此回滚事务。
    /// </para>
    /// <para>
    /// 三-4：<c>sender_bump</c> 通过 <c>RETURNING sent_count</c> 暴露递增后的发送计数，
    /// <c>msg_seq</c> 据此回写 <c>sender_sequence</c>。从 CTE 结果读取而非直接读表，
    /// 避免同语句内 UPDATE 与读取互相不可见的语义陷阱。
    /// </para>
    /// 返回新序列号；若会话不存在、非群、已解散或发送者非活跃成员则返回 null。
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
                  AND dissolved_at_ms IS NULL
                  AND EXISTS (
                      SELECT 1
                      FROM {schema.ConversationMembersTableSql}
                      WHERE conversation_id = @conversation_id
                        AND user_id = @sender_user_id
                        AND left_at_ms IS NULL
                  )
                RETURNING last_sequence
            ),
            sender_bump AS (
                UPDATE {schema.ConversationMembersTableSql}
                SET sent_count = sent_count + 1
                WHERE conversation_id = @conversation_id
                  AND user_id = @sender_user_id
                  AND left_at_ms IS NULL
                  AND EXISTS (SELECT 1 FROM advanced)
                RETURNING sent_count
            ),
            msg_seq AS (
                UPDATE {schema.MessagesTableSql}
                SET conversation_sequence = (SELECT last_sequence FROM advanced),
                    sender_sequence = (SELECT sent_count FROM sender_bump)
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
    /// Perf-1：单聊消息序列推进 O(1)。UPSERT 会话行 + 递增 last_sequence + 回写 message 序列
    /// （<c>conversation_sequence</c> 与 <c>sender_sequence</c>）+ 发送者 sent_count。
    /// 接收方 sent_count 不递增（接收方未发送）。
    /// <para>
    /// P0-1：不再在写入路径物化接收方 unread_count，也移除 receiver_unread CTE。
    /// unread_count 列仍由 MarkRead 路径（TryAdvanceReadBySequenceAsync）维护；
    /// 列表查询已基于序列公式派生未读数，不依赖此处的物化值。
    /// 与群聊行为一致：不在消息写入路径发送 per-user UnreadCountChanged 事件，
    /// 客户端通过 MessageReceived 事件中的 last_sequence 自行推导未读数变化。
    /// </para>
    /// <para>
    /// 三-4：<c>sender_upsert</c> 通过 <c>RETURNING sent_count</c> 暴露递增后的发送计数，
    /// <c>msg_seq</c> 据此回写 <c>sender_sequence</c>，供 MarkRead O(1) 查询使用。
    /// </para>
    /// 序列号始终递增（保证每条消息获得单调序列），但 tip（last_message_id/at_ms）仅在消息更新时前进，
    /// 支持乱序消息不回退 tip。
    /// 返回新序列号；若会话类型非单聊则返回 null（理论上不会发生，调用方已限定单聊路径）。
    /// </summary>
    public static async Task<long?> TryAdvanceDirectSequenceAsync(
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
                -- 三-4：RETURNING 递增后的 sent_count，供 msg_seq 回写 sender_sequence。
                INSERT INTO {schema.ConversationMembersTableSql} (
                    conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms, sent_count
                ) VALUES
                    (@conversation_id, @sender_user_id, @receiver_user_id, @received_at_ms, @received_at_ms, 1)
                ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                    sent_count = {schema.ConversationMembersTableSql}.sent_count + 1,
                    last_message_at_ms = @received_at_ms
                WHERE EXISTS (SELECT 1 FROM upsert_conversation)
                RETURNING sent_count
            ),
            msg_seq AS (
                UPDATE {schema.MessagesTableSql}
                SET conversation_sequence = (SELECT last_sequence FROM upsert_conversation),
                    sender_sequence = (SELECT sent_count FROM sender_upsert)
                WHERE message_id = @message_id
                  AND EXISTS (SELECT 1 FROM upsert_conversation)
            )
            SELECT last_sequence FROM upsert_conversation;
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

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long seq ? seq : (result is int i ? (long)i : null);
    }

    /// <summary>
    /// Perf-1：基于序列的 MarkRead。使用索引查询 + 单行 UPDATE。
    /// <para>
    /// unread = last_sequence - target_sequence - (sent_count - sent_upto_target)
    /// 其中 sent_upto_target = 用户在 target_sequence 之前发送的消息数。
    /// </para>
    /// <para>
    /// 三-4：sent_upto_target 不再使用 <c>COUNT(*)</c> 扫描，改为
    /// <c>ORDER BY conversation_sequence DESC LIMIT 1</c> 取目标序列前最后一条
    /// 发送消息的 <c>sender_sequence</c>（即递增计数），利用
    /// <c>ix_messages_sender_sequence_lookup</c> 索引 O(log N) 查找。
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
        // 读取会话 last_sequence 与成员当前 sent_count / last_read_sequence / 统一未读公式快照
        long conversationLastSequence;
        long memberSentCount;
        long? currentReadSequence;
        var currentUnread = 0;

        await using (var load = new NpgsqlCommand(
                         $"""
                          SELECT c.last_sequence,
                                 m.sent_count,
                                 m.last_read_sequence,
                                 {UnreadCountSqlExpression} AS unread_count
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
            currentUnread = reader.GetInt32(3);
        }

        // 已读到目标或更晚水位：无需推进。
        // P0-2：未读数使用统一序列公式（含 retention floor 与自发送消息扣除），
        // 不再用 last_sequence - crs 的简化公式（会多扣自发送消息、忽略 retention floor）。
        if (currentReadSequence is long crs && crs >= targetSequence)
        {
            return new SequenceReadAdvanceResult(
                Advanced: false,
                UnreadCount: currentUnread,
                LastSequence: conversationLastSequence);
        }

        // 索引查询：用户在 target_sequence 之前（含）发送的最后一条消息的 sender_sequence，
        // 即用户在目标序列前发送的消息总数。利用 ix_messages_sender_sequence_lookup 索引 O(log N) 查找，
        // 替代 O(N) 的 COUNT(*) 扫描。无匹配行时返回 0。
        long sentUptoTarget;
        await using (var sentCount = new NpgsqlCommand(
                         $"""
                          SELECT sender_sequence
                          FROM {schema.MessagesTableSql}
                          WHERE conversation_id = @conversation_id
                            AND sender_user_id = @user_id
                            AND conversation_sequence IS NOT NULL
                            AND conversation_sequence <= @target_sequence
                          ORDER BY conversation_sequence DESC
                          LIMIT 1;
                          """,
                         connection,
                         transaction))
        {
            sentCount.Parameters.AddWithValue("conversation_id", conversationId);
            sentCount.Parameters.AddWithValue("user_id", userId);
            sentCount.Parameters.AddWithValue("target_sequence", targetSequence);
            var sentObj = await sentCount.ExecuteScalarAsync(ct).ConfigureAwait(false);
            sentUptoTarget = sentObj is long v ? v : (sentObj is int i ? (long)i : 0);
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
            update.Parameters.AddWithValue("sent_upto_target", sentUptoTarget);
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

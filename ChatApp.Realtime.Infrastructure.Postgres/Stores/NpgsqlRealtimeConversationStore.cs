using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeConversationStore : IRealtimeConversationStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeConversationStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default)
    {
        take = Math.Clamp(take, 1, 101);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // 排序键与游标必须一致：置顶 → 置顶时间 → 最后消息时间 → ConversationId。
        // NULLS LAST 用 MinValue 哨兵，保证 keyset 比较与 ORDER BY 语义一致。
        const long nullSortSentinel = long.MinValue;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 c.conversation_id,
                 c.type,
                 m.peer_user_id,
                 c.last_message_id,
                 c.last_message_preview,
                 COALESCE(m.last_message_at_ms, c.last_message_at_ms) AS last_message_at_ms,
                 c.last_sender_user_id,
                 m.unread_count,
                 m.last_read_message_id,
                 m.last_read_at_ms,
                 m.is_pinned,
                 m.pinned_at_ms,
                 m.is_muted,
                 m.muted_until_ms
             FROM {_databaseSchema.ConversationMembersTableSql} AS m
             INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                 ON c.conversation_id = m.conversation_id
             WHERE m.user_id = @user_id
               AND (
                    @before_id IS NULL
                    OR (
                        m.is_pinned::int,
                        COALESCE(m.pinned_at_ms, {nullSortSentinel}),
                        COALESCE(m.last_message_at_ms, c.last_message_at_ms, {nullSortSentinel}),
                        c.conversation_id
                    ) < (
                        @before_pinned::int,
                        COALESCE(@before_pinned_at, {nullSortSentinel}),
                        COALESCE(@before_at, {nullSortSentinel}),
                        @before_id
                    )
               )
             ORDER BY
                 m.is_pinned DESC,
                 m.pinned_at_ms DESC NULLS LAST,
                 COALESCE(m.last_message_at_ms, c.last_message_at_ms) DESC NULLS LAST,
                 c.conversation_id DESC
             LIMIT @take;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        var beforePinned = command.Parameters.Add("before_pinned", NpgsqlDbType.Boolean);
        beforePinned.Value = (object?)beforeIsPinned ?? DBNull.Value;
        var beforePinnedAt = command.Parameters.Add("before_pinned_at", NpgsqlDbType.Bigint);
        beforePinnedAt.Value = (object?)beforePinnedAtMs ?? DBNull.Value;
        var beforeAt = command.Parameters.Add("before_at", NpgsqlDbType.Bigint);
        beforeAt.Value = (object?)beforeLastMessageAtMs ?? DBNull.Value;
        var beforeId = command.Parameters.Add("before_id", NpgsqlDbType.Text);
        beforeId.Value = (object?)beforeConversationId ?? DBNull.Value;
        command.Parameters.AddWithValue("take", take);

        var items = new List<ConversationListItem>(take);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new ConversationListItem
            {
                ConversationId = reader.GetString(0),
                Type = (ConversationType)reader.GetInt16(1),
                PeerUserId = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                LastMessageId = reader.IsDBNull(3) ? null : reader.GetString(3),
                LastMessagePreview = reader.IsDBNull(4) ? null : reader.GetString(4),
                LastMessageAtMs = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                LastSenderUserId = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                UnreadCount = reader.GetInt32(7),
                LastReadMessageId = reader.IsDBNull(8) ? null : reader.GetString(8),
                LastReadAtMs = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                IsPinned = reader.GetBoolean(10),
                PinnedAtMs = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                IsMuted = reader.GetBoolean(12),
                MutedUntilMs = reader.IsDBNull(13) ? null : reader.GetInt64(13)
            });
        }

        return items;
    }

    public async Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
        long userId,
        string conversationId,
        bool? pinned,
        bool? muted,
        long? mutedUntilMs,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        ConversationType type;
        long? peerUserId;
        string? lastMessageId;
        string? lastMessagePreview;
        long? lastMessageAtMs;
        long? lastSenderUserId;
        bool currentPinned;
        long? currentPinnedAtMs;
        bool currentMuted;
        long? currentMutedUntilMs;

        await using (var load = new NpgsqlCommand(
                           $"""
                            SELECT
                                c.type,
                                m.peer_user_id,
                                c.last_message_id,
                                c.last_message_preview,
                                c.last_message_at_ms,
                                c.last_sender_user_id,
                                m.is_pinned,
                                m.pinned_at_ms,
                                m.is_muted,
                                m.muted_until_ms
                            FROM {_databaseSchema.ConversationMembersTableSql} AS m
                            INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                                ON c.conversation_id = m.conversation_id
                            WHERE m.conversation_id = @conversation_id
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
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return new ConversationMemberPrefsResult(false, false, false, false, null);
            }

            type = (ConversationType)reader.GetInt16(0);
            peerUserId = reader.IsDBNull(1) ? null : reader.GetInt64(1);
            lastMessageId = reader.IsDBNull(2) ? null : reader.GetString(2);
            lastMessagePreview = reader.IsDBNull(3) ? null : reader.GetString(3);
            lastMessageAtMs = reader.IsDBNull(4) ? null : reader.GetInt64(4);
            lastSenderUserId = reader.IsDBNull(5) ? null : reader.GetInt64(5);
            currentPinned = reader.GetBoolean(6);
            currentPinnedAtMs = reader.IsDBNull(7) ? null : reader.GetInt64(7);
            currentMuted = reader.GetBoolean(8);
            currentMutedUntilMs = reader.IsDBNull(9) ? null : reader.GetInt64(9);
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nextPinned = currentPinned;
        var nextPinnedAtMs = currentPinnedAtMs;
        var nextMuted = currentMuted;
        var nextMutedUntilMs = currentMutedUntilMs;

        if (pinned is true)
        {
            nextPinned = true;
            // 仅 false→true 刷新 pinned_at_ms；重复 pin 不改时间、不产生 Outbox。
            if (!currentPinned)
                nextPinnedAtMs = now;
        }
        else if (pinned is false)
        {
            nextPinned = false;
            nextPinnedAtMs = null;
        }

        if (muted is true)
        {
            nextMuted = true;
            nextMutedUntilMs = mutedUntilMs;
        }
        else if (muted is false)
        {
            nextMuted = false;
            nextMutedUntilMs = null;
        }

        var changed = nextPinned != currentPinned
            || nextPinnedAtMs != currentPinnedAtMs
            || nextMuted != currentMuted
            || nextMutedUntilMs != currentMutedUntilMs;

        if (!changed)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new ConversationMemberPrefsResult(
                true,
                false,
                currentPinned,
                currentMuted,
                currentMutedUntilMs);
        }

        await using (var update = new NpgsqlCommand(
                           $"""
                            UPDATE {_databaseSchema.ConversationMembersTableSql}
                            SET is_pinned = @is_pinned,
                                pinned_at_ms = @pinned_at_ms,
                                is_muted = @is_muted,
                                muted_until_ms = @muted_until_ms
                            WHERE conversation_id = @conversation_id
                              AND user_id = @user_id;
                            """,
                           connection,
                           transaction))
        {
            update.Parameters.AddWithValue("is_pinned", nextPinned);
            update.Parameters.AddWithValue("pinned_at_ms", (object?)nextPinnedAtMs ?? DBNull.Value);
            update.Parameters.AddWithValue("is_muted", nextMuted);
            update.Parameters.AddWithValue("muted_until_ms", (object?)nextMutedUntilMs ?? DBNull.Value);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        await InsertOutboxAsync(
                connection,
                transaction,
                ConversationWriteCommands.CreateConversationPrefsChangedEvent(
                    conversationId,
                    userId,
                    type,
                    peerUserId,
                    lastMessageId,
                    lastMessagePreview,
                    lastMessageAtMs,
                    lastSenderUserId,
                    nextPinned,
                    nextPinnedAtMs,
                    nextMuted,
                    nextMutedUntilMs,
                    now,
                    traceParent,
                    traceState),
                ct)
            .ConfigureAwait(false);

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return new ConversationMemberPrefsResult(
            true,
            true,
            nextPinned,
            nextMuted,
            nextMutedUntilMs);
    }

    public async Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
        long userId,
        string conversationId,
        long? readAtMs,
        string? readMessageId,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        _ = readAtMs; // 权威时间由 messageId 解析；保留参数以兼容调用方。

        long? tipAtMs;
        string? tipMessageId;
        long? currentReadAtMs;
        string? currentReadMessageId;
        int currentUnread;

        await using (var load = new NpgsqlCommand(
                           $"""
                            SELECT
                                c.last_message_at_ms,
                                c.last_message_id,
                                m.last_read_at_ms,
                                m.last_read_message_id,
                                m.unread_count
                            FROM {_databaseSchema.ConversationMembersTableSql} AS m
                            INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                                ON c.conversation_id = m.conversation_id
                            WHERE m.conversation_id = @conversation_id
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
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return new ConversationReadAdvanceResult(false, false, 0, null, null);
            }

            tipAtMs = reader.IsDBNull(0) ? null : reader.GetInt64(0);
            tipMessageId = reader.IsDBNull(1) ? null : reader.GetString(1);
            currentReadAtMs = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            currentReadMessageId = reader.IsDBNull(3) ? null : reader.GetString(3);
            currentUnread = reader.GetInt32(4);
        }

        long targetAtMs;
        string targetMessageId;
        // messageId 为空：推进到 tip。非空：以库内时间为权威，忽略客户端 readAtMs。
        if (string.IsNullOrWhiteSpace(readMessageId))
        {
            if (tipAtMs is null || string.IsNullOrWhiteSpace(tipMessageId))
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new ConversationReadAdvanceResult(
                    true,
                    false,
                    currentUnread,
                    currentReadMessageId,
                    currentReadAtMs);
            }

            targetAtMs = tipAtMs.Value;
            targetMessageId = tipMessageId;
        }
        else
        {
            var resolved = await TryResolveReadCursorMessageAsync(
                    connection,
                    transaction,
                    conversationId,
                    readMessageId,
                    ct)
                .ConfigureAwait(false);
            if (resolved is null)
            {
                // 消息不存在或不属于该会话：忽略客户端游标，不前进。
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new ConversationReadAdvanceResult(
                    true,
                    false,
                    currentUnread,
                    currentReadMessageId,
                    currentReadAtMs);
            }

            targetAtMs = resolved.Value.ReceivedAtMs;
            targetMessageId = resolved.Value.MessageId;

            // 限制在当前会话 tip 以内，防止未来游标把后续消息全部视作已读。
            if (tipAtMs is not null
                && !string.IsNullOrWhiteSpace(tipMessageId)
                && IsMessageAfter(targetAtMs, targetMessageId, tipAtMs.Value, tipMessageId))
            {
                targetAtMs = tipAtMs.Value;
                targetMessageId = tipMessageId;
            }
        }

        var shouldAdvance = currentReadAtMs is null
            || currentReadAtMs.Value < targetAtMs
            || (currentReadAtMs.Value == targetAtMs
                && (currentReadMessageId is null
                    || string.CompareOrdinal(currentReadMessageId, targetMessageId) < 0));
        if (!shouldAdvance)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new ConversationReadAdvanceResult(
                true,
                false,
                currentUnread,
                currentReadMessageId,
                currentReadAtMs);
        }

        var unread = await CountUnreadBoundedAsync(
                connection,
                transaction,
                conversationId,
                userId,
                targetAtMs,
                targetMessageId,
                ct)
            .ConfigureAwait(false);

        await using (var update = new NpgsqlCommand(
                           $"""
                            UPDATE {_databaseSchema.ConversationMembersTableSql}
                            SET last_read_at_ms = @read_at_ms,
                                last_read_message_id = @read_message_id,
                                unread_count = @unread_count
                            WHERE conversation_id = @conversation_id
                              AND user_id = @user_id;
                            """,
                           connection,
                           transaction))
        {
            update.Parameters.AddWithValue("read_at_ms", targetAtMs);
            update.Parameters.AddWithValue("read_message_id", targetMessageId);
            update.Parameters.AddWithValue("unread_count", unread);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        await InsertOutboxAsync(
                connection,
                transaction,
                ConversationWriteCommands.CreateUnreadCountChangedEvent(
                    conversationId,
                    userId,
                    unread,
                    targetMessageId,
                    targetAtMs,
                    causeMessageId: targetMessageId,
                    now,
                    traceParent,
                    traceState),
                ct)
            .ConfigureAwait(false);

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return new ConversationReadAdvanceResult(
            true,
            true,
            unread,
            targetMessageId,
            targetAtMs);
    }

    private async Task<(long ReceivedAtMs, string MessageId)?> TryResolveReadCursorMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        string messageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT received_at_ms, message_id
             FROM {_databaseSchema.MessagesTableSql}
             WHERE conversation_id = @conversation_id
               AND message_id = @message_id
             LIMIT 1;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("message_id", messageId.Trim());
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return (reader.GetInt64(0), reader.GetString(1));
    }

    private static bool IsMessageAfter(
        long candidateAtMs,
        string candidateMessageId,
        long tipAtMs,
        string tipMessageId) =>
        candidateAtMs > tipAtMs
        || (candidateAtMs == tipAtMs
            && string.CompareOrdinal(candidateMessageId, tipMessageId) > 0);

    private async Task<int> CountUnreadBoundedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long userId,
        long readAtMs,
        string readMessageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM (
                 SELECT 1
                 FROM {_databaseSchema.MessagesTableSql}
                 WHERE conversation_id = @conversation_id
                   AND sender_user_id <> @user_id
                   AND (
                        received_at_ms > @read_at_ms
                        OR (received_at_ms = @read_at_ms AND message_id > @read_message_id)
                   )
                 LIMIT @max_unread
             ) AS bounded;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("read_at_ms", readAtMs);
        command.Parameters.AddWithValue("read_message_id", readMessageId);
        command.Parameters.AddWithValue("max_unread", ConversationWriteCommands.MaxTrackedUnreadCount);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int count ? count : Convert.ToInt32(result);
    }

    private async Task InsertOutboxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload_json, @target_user_id, @event_type, @status,
                 @created_at_ms, @next_attempt_at_ms, 0
             )
             ON CONFLICT (event_id) DO NOTHING;
             """,
            connection,
            transaction);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        command.Parameters.AddWithValue("event_id", evt.EventId);
        command.Parameters.AddWithValue(
            "payload_json",
            JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
        command.Parameters.AddWithValue("target_user_id", evt.TargetUserId);
        command.Parameters.AddWithValue("event_type", (short)evt.Type);
        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

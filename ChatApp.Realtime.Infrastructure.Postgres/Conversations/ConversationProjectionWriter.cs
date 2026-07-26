using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Conversations;

/// <summary>
/// 会话投影写入：在当前事务内推进会话 tip、递增未读、列出群成员、推进已读水位，
/// 并构造对应的 <see cref="RealtimeEventType.ConversationListChanged"/> /
/// <see cref="RealtimeEventType.UnreadCountChanged"/> 事件。
/// 薄封装 <see cref="ConversationWriteCommands"/>，使其以 Writer 形式参与共享事务编排。
/// </summary>
internal sealed class ConversationProjectionWriter
{
    private readonly RealtimeWriteSession _session;

    public ConversationProjectionWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    public Task<(bool Advanced, int? UnreadCount)> TryAdvanceAndIncrementUnreadAsync(
        string conversationId,
        long senderUserId,
        long receiverUserId,
        string messageId,
        string preview,
        long receivedAtMs) =>
        ConversationWriteCommands.TryAdvanceAndIncrementUnreadAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            conversationId,
            senderUserId,
            receiverUserId,
            messageId,
            preview,
            receivedAtMs,
            _session.CancellationToken);

    public Task<(bool Advanced, IReadOnlyList<(long UserId, int UnreadCount)> Unreads)>
        TryAdvanceGroupAndIncrementUnreadAsync(
            string conversationId,
            long senderUserId,
            string messageId,
            string preview,
            long receivedAtMs) =>
        ConversationWriteCommands.TryAdvanceGroupAndIncrementUnreadAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            conversationId,
            senderUserId,
            messageId,
            preview,
            receivedAtMs,
            _session.CancellationToken);

    public Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(string conversationId) =>
        ConversationWriteCommands.ListActiveMemberUserIdsAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            conversationId,
            _session.CancellationToken);

    /// <summary>
    /// 当被撤回/编辑的消息恰为会话 tip 时，更新 tip 摘要；返回是否命中并更新。
    /// </summary>
    public async Task<bool> TryUpdateConversationTipPreviewAsync(
        string conversationId,
        string messageId,
        string preview)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_session.Schema.ConversationsTableSql}
             SET last_message_preview = @preview
             WHERE conversation_id = @conversation_id
               AND last_message_id = @message_id
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("preview", preview);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("message_id", messageId);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) > 0;
    }

    /// <summary>
    /// 推进单聊/群成员的已读水位，并返回需投递的未读变更事件。
    /// 仅当新水位严格晚于当前水位（或同水位下消息编号更大）时才推进。
    /// </summary>
    public async Task<RealtimeEvent?> TryAdvanceReadStateAsync(
        long userId,
        string conversationId,
        long readAtMs,
        string readMessageId)
    {
        var ct = _session.CancellationToken;
        long? currentReadAtMs;
        string? currentReadMessageId;

        await using (var load = new NpgsqlCommand(
                         $"""
                          SELECT last_read_at_ms, last_read_message_id
                          FROM {_session.Schema.ConversationMembersTableSql}
                          WHERE conversation_id = @conversation_id
                            AND user_id = @user_id
                          FOR UPDATE;
                          """,
                         _session.Connection,
                         _session.Transaction))
        {
            load.Parameters.AddWithValue("conversation_id", conversationId);
            load.Parameters.AddWithValue("user_id", userId);
            await using var reader = await load.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return null;

            currentReadAtMs = reader.IsDBNull(0) ? null : reader.GetInt64(0);
            currentReadMessageId = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        var shouldAdvance = currentReadAtMs is null
            || currentReadAtMs.Value < readAtMs
            || (currentReadAtMs.Value == readAtMs
                && (currentReadMessageId is null
                    || string.CompareOrdinal(currentReadMessageId, readMessageId) < 0));
        if (!shouldAdvance)
            return null;

        await using var countCmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM (
                 SELECT 1
                 FROM {_session.Schema.MessagesTableSql}
                 WHERE conversation_id = @conversation_id
                   AND sender_user_id <> @user_id
                   AND (
                        received_at_ms > @read_at_ms
                        OR (received_at_ms = @read_at_ms AND message_id > @read_message_id)
                   )
                 LIMIT @max_unread
             ) AS bounded;
             """,
            _session.Connection,
            _session.Transaction);
        countCmd.Parameters.AddWithValue("conversation_id", conversationId);
        countCmd.Parameters.AddWithValue("user_id", userId);
        countCmd.Parameters.AddWithValue("read_at_ms", readAtMs);
        countCmd.Parameters.AddWithValue("read_message_id", readMessageId);
        countCmd.Parameters.AddWithValue(
            "max_unread",
            ConversationWriteCommands.MaxTrackedUnreadCount);
        var unreadObj = await countCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        var unread = unreadObj is int value ? value : Convert.ToInt32(unreadObj);

        await using (var update = new NpgsqlCommand(
                           $"""
                            UPDATE {_session.Schema.ConversationMembersTableSql}
                            SET last_read_at_ms = @read_at_ms,
                                last_read_message_id = @read_message_id,
                                unread_count = @unread_count
                            WHERE conversation_id = @conversation_id
                              AND user_id = @user_id;
                            """,
                           _session.Connection,
                           _session.Transaction))
        {
            update.Parameters.AddWithValue("read_at_ms", readAtMs);
            update.Parameters.AddWithValue("read_message_id", readMessageId);
            update.Parameters.AddWithValue("unread_count", unread);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        return ConversationWriteCommands.CreateUnreadCountChangedEvent(
            conversationId,
            userId,
            unread,
            readMessageId,
            readAtMs,
            causeMessageId: readMessageId,
            readAtMs,
            traceParent: null,
            traceState: null);
    }
}

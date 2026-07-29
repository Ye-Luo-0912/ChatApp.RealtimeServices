using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messages;

/// <summary>
/// 实时消息行写入与幂等读：在当前事务内 INSERT 消息（<c>ON CONFLICT DO NOTHING</c>），
/// 读取冲突行以做指纹比对，并列出消息已绑定的附件编号。
/// </summary>
internal sealed class MessageWriter
{
    private readonly RealtimeWriteSession _session;

    public MessageWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    /// <summary>
    /// 写入消息行；返回受影响行数（0 表示命中 sender+client_message_id 幂等键）。
    /// </summary>
    public async Task<int> InsertAsync(RealtimeMessageRecord message, string fingerprint)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_session.Schema.MessagesTableSql} (
                message_id,
                client_message_id,
                sender_user_id,
                sender_session_id,
                receiver_user_id,
                conversation_id,
                content,
                content_fingerprint,
                received_at_ms,
                created_at_ms,
                reply_to_message_id,
                reply_to_sender_user_id,
                reply_to_preview,
                forwarded_from_message_id,
                forwarded_from_sender_user_id,
                forwarded_from_preview,
                mentioned_user_ids,
                mentioned_roles,
                edit_version,
                changed_at_ms
            )
            VALUES (
                @message_id,
                @client_message_id,
                @sender_user_id,
                @sender_session_id,
                @receiver_user_id,
                @conversation_id,
                @content,
                @content_fingerprint,
                @received_at_ms,
                @created_at_ms,
                @reply_to_message_id,
                @reply_to_sender_user_id,
                @reply_to_preview,
                @forwarded_from_message_id,
                @forwarded_from_sender_user_id,
                @forwarded_from_preview,
                @mentioned_user_ids,
                @mentioned_roles,
                1,
                @received_at_ms
            )
            ON CONFLICT (sender_user_id, client_message_id) DO NOTHING;
            """,
            _session.Connection,
            _session.Transaction);

        command.Parameters.AddWithValue("message_id", message.MessageId);
        command.Parameters.AddWithValue("client_message_id", message.ClientMessageId);
        command.Parameters.AddWithValue("sender_user_id", message.SenderUserId);
        command.Parameters.AddWithValue("sender_session_id", message.SenderSessionId);
        command.Parameters.AddWithValue("receiver_user_id", message.ReceiverUserId);
        command.Parameters.AddWithValue(
            "conversation_id",
            (object?)message.ConversationId ?? DBNull.Value);
        command.Parameters.AddWithValue("content", message.Content);
        command.Parameters.AddWithValue("content_fingerprint", fingerprint);
        command.Parameters.AddWithValue("received_at_ms", message.ReceivedAtMs);
        command.Parameters.AddWithValue("created_at_ms", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        command.Parameters.AddWithValue(
            "reply_to_message_id",
            (object?)message.ReplyToMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "reply_to_sender_user_id",
            message.ReplyToSenderUserId.HasValue
                ? message.ReplyToSenderUserId.Value
                : DBNull.Value);
        command.Parameters.AddWithValue(
            "reply_to_preview",
            (object?)message.ReplyToPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "forwarded_from_message_id",
            (object?)message.ForwardedFromMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "forwarded_from_sender_user_id",
            message.ForwardedFromSenderUserId.HasValue
                ? message.ForwardedFromSenderUserId.Value
                : DBNull.Value);
        command.Parameters.AddWithValue(
            "forwarded_from_preview",
            (object?)message.ForwardedFromPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            "mentioned_user_ids",
            message.MentionedUserIds is { Count: > 0 }
                ? (object)message.MentionedUserIds.ToArray()
                : DBNull.Value);
        command.Parameters.AddWithValue(
            "mentioned_roles",
            message.MentionedRoles is { Count: > 0 }
                ? (object)message.MentionedRoles.ToArray()
                : DBNull.Value);

        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<ExistingMessage> GetExistingForIdempotencyAsync(
        long senderUserId,
        string clientMessageId)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT message_id, receiver_user_id, content, content_fingerprint,
                    conversation_id, reply_to_message_id, forwarded_from_message_id,
                    mentioned_user_ids, mentioned_roles,
                    reply_to_sender_user_id, reply_to_preview,
                    forwarded_from_sender_user_id, forwarded_from_preview
             FROM {_session.Schema.MessagesTableSql}
             WHERE sender_user_id = @sender_user_id AND client_message_id = @client_message_id
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("client_message_id", clientMessageId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException("检测到消息冲突，但无法读取已有消息编号。");

        // P0-8/P0-10：读取 v4 指纹覆盖字段（含 Reply/Forward 的 sender/preview），用于旧版本指纹的重算比对
        return new ExistingMessage(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<long[]>(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<string[]>(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetInt64(11),
            reader.IsDBNull(12) ? null : reader.GetString(12));
    }

    public async Task<IReadOnlyList<string>> ListAttachmentIdsAsync(string messageId)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id
             FROM {_session.Schema.AttachmentsTableSql}
             WHERE message_id = @message_id
             ORDER BY attachment_id;
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        var ids = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            ids.Add(reader.GetString(0));
        return ids;
    }

    public sealed record ExistingMessage(
        string MessageId,
        long ReceiverUserId,
        string Content,
        string? Fingerprint,
        string? ConversationId,
        string? ReplyToMessageId,
        string? ForwardedFromMessageId,
        long[]? MentionedUserIds,
        string[]? MentionedRoles,
        long? ReplyToSenderUserId = null,
        string? ReplyToPreview = null,
        long? ForwardedFromSenderUserId = null,
        string? ForwardedFromPreview = null);
}

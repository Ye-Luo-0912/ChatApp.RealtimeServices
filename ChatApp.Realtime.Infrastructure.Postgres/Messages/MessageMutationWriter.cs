using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messages;

/// <summary>
/// 消息变更（撤回 / 编辑）写入：在当前事务内读取消息行（FOR UPDATE）、写入幂等
/// <c>message_mutation_requests</c> 记录、应用 UPDATE、推进会话 tip 摘要。
/// 包含变更请求的读、写与结果映射，避免 <c>NpgsqlRealtimeMessageStore</c> 直接持有 SQL。
/// </summary>
internal sealed class MessageMutationWriter
{
    private readonly RealtimeWriteSession _session;

    public MessageMutationWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    public async Task<MutationRequestRow?> TryReadMutationRequestAsync(
        long actorUserId,
        string requestId)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT operation, message_id, payload_fingerprint, succeeded, error_code,
                    conversation_id, content, edit_version, edited_at_ms, recalled_at_ms
             FROM {_session.Schema.MessageMutationRequestsTableSql}
             WHERE actor_user_id = @actor_user_id
               AND request_id = @request_id
             FOR UPDATE
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new MutationRequestRow(
            reader.GetInt16(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetBoolean(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetInt32(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9));
    }

    public async Task InsertMutationRequestAsync(
        long actorUserId,
        string requestId,
        short operation,
        string messageId,
        string payloadFingerprint,
        bool succeeded,
        string? errorCode,
        string? conversationId,
        string? content,
        int? editVersion,
        long? editedAtMs,
        long? recalledAtMs)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_session.Schema.MessageMutationRequestsTableSql} (
                 actor_user_id,
                 request_id,
                 operation,
                 message_id,
                 payload_fingerprint,
                 succeeded,
                 error_code,
                 conversation_id,
                 content,
                 edit_version,
                 edited_at_ms,
                 recalled_at_ms,
                 created_at_ms
             )
             VALUES (
                 @actor_user_id,
                 @request_id,
                 @operation,
                 @message_id,
                 @payload_fingerprint,
                 @succeeded,
                 @error_code,
                 @conversation_id,
                 @content,
                 @edit_version,
                 @edited_at_ms,
                 @recalled_at_ms,
                 @created_at_ms
             )
             ON CONFLICT (actor_user_id, request_id) DO NOTHING;
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        command.Parameters.AddWithValue("operation", operation);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("payload_fingerprint", payloadFingerprint);
        command.Parameters.AddWithValue("succeeded", succeeded);
        command.Parameters.AddWithValue("error_code", (object?)errorCode ?? DBNull.Value);
        command.Parameters.AddWithValue("conversation_id", (object?)conversationId ?? DBNull.Value);
        command.Parameters.AddWithValue("content", (object?)content ?? DBNull.Value);
        command.Parameters.AddWithValue("edit_version", editVersion.HasValue ? editVersion.Value : DBNull.Value);
        command.Parameters.AddWithValue("edited_at_ms", editedAtMs.HasValue ? editedAtMs.Value : DBNull.Value);
        command.Parameters.AddWithValue("recalled_at_ms", recalledAtMs.HasValue ? recalledAtMs.Value : DBNull.Value);
        command.Parameters.AddWithValue(
            "created_at_ms",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<RecallTargetRow?> ReadMessageForRecallAsync(string messageId)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT sender_user_id, receiver_user_id, conversation_id, received_at_ms, recalled_at_ms
             FROM {_session.Schema.MessagesTableSql}
             WHERE message_id = @message_id
             FOR UPDATE
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new RecallTargetRow(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4));
    }

    public async Task<int> ApplyRecallUpdateAsync(string messageId, long recalledAtMs)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_session.Schema.MessagesTableSql}
             SET content = '',
                 recalled_at_ms = @recalled_at_ms,
                 changed_at_ms = @recalled_at_ms,
                 reply_to_message_id = NULL,
                 reply_to_sender_user_id = NULL,
                 reply_to_preview = NULL,
                 forwarded_from_message_id = NULL,
                 forwarded_from_sender_user_id = NULL,
                 forwarded_from_preview = NULL
             WHERE message_id = @message_id
               AND recalled_at_ms IS NULL
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("recalled_at_ms", recalledAtMs);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<EditTargetRow?> ReadMessageForEditAsync(string messageId)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT sender_user_id, receiver_user_id, conversation_id, received_at_ms,
                    recalled_at_ms, content, edit_version
             FROM {_session.Schema.MessagesTableSql}
             WHERE message_id = @message_id
             FOR UPDATE
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new EditTargetRow(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4),
            reader.GetString(5),
            reader.GetInt32(6));
    }

    public async Task<int> ApplyEditUpdateAsync(
        string messageId,
        string content,
        int editVersion,
        long editedAtMs)
    {
        var ct = _session.CancellationToken;
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_session.Schema.MessagesTableSql}
             SET content = @content,
                 edit_version = @edit_version,
                 edited_at_ms = @edited_at_ms,
                 changed_at_ms = @edited_at_ms
             WHERE message_id = @message_id
               AND recalled_at_ms IS NULL
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("content", content);
        command.Parameters.AddWithValue("edit_version", editVersion);
        command.Parameters.AddWithValue("edited_at_ms", editedAtMs);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static string ComputeMutationFingerprint(short operation, string messageId, string content)
    {
        var input = System.Text.Encoding.UTF8.GetBytes($"{operation}\n{messageId}\n{content}");
        return Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(input));
    }

    public static MessageRecallPersistResult MapRecallFailure(
        string? errorCode,
        string messageId,
        string? conversationId) =>
        errorCode switch
        {
            "message_not_found" => new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotFound,
                messageId),
            "recall_not_allowed" => new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                ConversationId: conversationId),
            "recall_window_expired" => new MessageRecallPersistResult(
                MessageRecallPersistStatus.WindowExpired,
                messageId,
                ConversationId: conversationId),
            _ => new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                ConversationId: conversationId)
        };

    public static MessageEditPersistResult MapEditFailure(
        string? errorCode,
        string messageId,
        string? conversationId) =>
        errorCode switch
        {
            "message_not_found" => new MessageEditPersistResult(
                MessageEditPersistStatus.NotFound,
                messageId),
            "edit_not_allowed" => new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                ConversationId: conversationId),
            "edit_window_expired" => new MessageEditPersistResult(
                MessageEditPersistStatus.WindowExpired,
                messageId,
                ConversationId: conversationId),
            "message_recalled" => new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                ConversationId: conversationId),
            _ => new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                ConversationId: conversationId)
        };

    public sealed record MutationRequestRow(
        short Operation,
        string MessageId,
        string PayloadFingerprint,
        bool Succeeded,
        string? ErrorCode,
        string? ConversationId,
        string? Content,
        int? EditVersion,
        long? EditedAtMs,
        long? RecalledAtMs);

    public sealed record RecallTargetRow(
        long SenderUserId,
        long ReceiverUserId,
        string? ConversationId,
        long ReceivedAtMs,
        long? RecalledAtMs);

    public sealed record EditTargetRow(
        long SenderUserId,
        long ReceiverUserId,
        string? ConversationId,
        long ReceivedAtMs,
        long? RecalledAtMs,
        string Content,
        int EditVersion);
}

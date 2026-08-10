using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messages;

/// <summary>
/// 无附件消息的原子创建写入器：用单条数据修改 CTE 同时写入 message、Outbox 与可选幂等账本。
/// </summary>
/// <remarks>
/// 仅共享不可变 SQL 文本；连接、事务、命令、参数和 reader 均属于当前
/// <see cref="RealtimeWriteSession"/>，不会跨请求共享有状态对象。Outbox 与 ledger 都以
/// <c>inserted_message</c> 为数据源，因此消息幂等冲突时不会产生孤立事件或账本记录。
/// </remarks>
internal sealed class MessageCreateBundleWriter(RealtimeWriteSession session)
{
    public async Task<MessageCreateBundleResult> InsertAsync(
        RealtimeMessageRecord message,
        string fingerprint,
        long? conversationSequence,
        long? senderSequence,
        RealtimeEvent evt,
        bool writeLedger)
    {
        RealtimeOutboxPreclaim? preclaim = null;
        if (evt.TargetUserIds is { Length: > 0 }
            && session.TryReserveOutboxPreclaim(evt.EventId, out var reserved))
        {
            preclaim = reserved;
        }

        var commandText = session.Schema.GetOrAddCommandText(
            "insert-message-outbox-ledger-bundle",
            static schema => $"""
            WITH inserted_message AS MATERIALIZED (
                INSERT INTO {schema.MessagesTableSql} (
                    message_id, client_message_id, sender_user_id, sender_session_id,
                    receiver_user_id, conversation_id, content, content_fingerprint,
                    received_at_ms, created_at_ms, reply_to_message_id,
                    reply_to_sender_user_id, reply_to_preview, forwarded_from_message_id,
                    forwarded_from_sender_user_id, forwarded_from_preview,
                    mentioned_user_ids, mentioned_roles, edit_version, changed_at_ms,
                    conversation_sequence, sender_sequence
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11,
                    $12, $13, $14,
                    $15, $16,
                    $17, $18, 1, $9,
                    $19, $20
                )
                ON CONFLICT (sender_user_id, client_message_id) DO NOTHING
                RETURNING message_id
            ),
            inserted_outbox AS MATERIALIZED (
                INSERT INTO {schema.OutboxTableSql} (
                    event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                    created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
                    audience_kind, conversation_id, exclude_user_id, trace_parent, trace_state,
                    occurred_at_ms, locked_by, locked_until_ms, claim_token
                )
                SELECT
                    $21, NULL, $22, $23, $24, $25,
                    $10, COALESCE($35, $10), $26, $27,
                    $28, $29, NULLIF($30, 0),
                    $31, $32, $33,
                    $34, $35, $36
                FROM inserted_message
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            ),
            inserted_ledger AS MATERIALIZED (
                INSERT INTO {schema.CommandIdempotencyLedgerTableSql} (
                    sender_user_id, client_message_id, command_id, content_fingerprint,
                    result_kind, message_id, received_at_ms
                )
                SELECT
                    $3, $2, $1, $8,
                    $38, $1, $9
                FROM inserted_message
                WHERE $37
                ON CONFLICT (sender_user_id, client_message_id) DO NOTHING
                RETURNING sender_user_id
            )
            SELECT
                (CASE WHEN EXISTS (SELECT 1 FROM inserted_message) THEN 1 ELSE 0 END)
                + (CASE WHEN EXISTS (SELECT 1 FROM inserted_outbox) THEN 2 ELSE 0 END)
                + (CASE WHEN EXISTS (SELECT 1 FROM inserted_ledger) THEN 4 ELSE 0 END);
            """);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var payloadUtf8 = OutboxInsertHelper.SerializeWirePayloadToUtf8(evt);

        await using var command = new NpgsqlCommand(commandText, session.Connection, session.Transaction);
        AddMessageParameters(command, message, fingerprint, conversationSequence, senderSequence, now);
        AddOutboxParameters(command, evt, payloadUtf8, preclaim);
        command.Parameters.AddWithValue(NpgsqlDbType.Boolean, writeLedger);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)IdempotencyLedgerResultKind.Created);

        var scalar = await command.ExecuteScalarAsync(session.CancellationToken).ConfigureAwait(false);
        if (scalar is null or DBNull)
            throw new InvalidOperationException("原子消息创建命令未返回写入结果。");

        var flags = scalar is int value ? value : Convert.ToInt32(scalar);
        var result = new MessageCreateBundleResult(
            MessageInserted: (flags & 1) != 0,
            OutboxInserted: (flags & 2) != 0,
            LedgerInserted: (flags & 4) != 0);

        session.RecordOutboxInsert(result.OutboxInserted ? 1 : 0, evt, preclaim);
        return result;
    }

    private static void AddMessageParameters(
        NpgsqlCommand command,
        RealtimeMessageRecord message,
        string fingerprint,
        long? conversationSequence,
        long? senderSequence,
        long now)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.MessageId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.ClientMessageId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.SenderUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.SenderSessionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.ReceiverUserId);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ConversationId ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, message.Content);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, fingerprint);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, message.ReceivedAtMs);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, now);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ReplyToMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)message.ReplyToSenderUserId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ReplyToPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ForwardedFromMessageId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)message.ForwardedFromSenderUserId ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)message.ForwardedFromPreview ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            message.MentionedUserIds is { Count: > 0 }
                ? message.MentionedUserIds.ToArray()
                : DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Text,
            message.MentionedRoles is { Count: > 0 }
                ? message.MentionedRoles.ToArray()
                : DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)conversationSequence ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)senderSequence ?? DBNull.Value);
    }

    private static void AddOutboxParameters(
        NpgsqlCommand command,
        RealtimeEvent evt,
        byte[] payloadUtf8,
        RealtimeOutboxPreclaim? preclaim)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, evt.EventId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bytea, payloadUtf8);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.TargetUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Smallint, (short)evt.Type);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Integer,
            preclaim is null ? 0 : 1);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            (object?)evt.TargetUserIds ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Smallint,
            (short)(evt.AudienceKind ?? 0));
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)evt.ConversationId ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.ExcludeUserId ?? 0L);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)evt.TraceParent ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)evt.TraceState ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, evt.OccurredAtMs);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)preclaim?.LockOwner ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Bigint,
            (object?)preclaim?.LockedUntilMs ?? DBNull.Value);
        command.Parameters.AddWithValue(
            NpgsqlDbType.Text,
            (object?)preclaim?.ClaimToken ?? DBNull.Value);
    }
}

internal readonly record struct MessageCreateBundleResult(
    bool MessageInserted,
    bool OutboxInserted,
    bool LedgerInserted);

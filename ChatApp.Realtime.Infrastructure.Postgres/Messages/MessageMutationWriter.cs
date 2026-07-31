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
             SELECT sender_user_id, receiver_user_id, conversation_id, received_at_ms, recalled_at_ms,
                    conversation_sequence
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
            reader.IsDBNull(4) ? null : reader.GetInt64(4),
            reader.IsDBNull(5) ? null : reader.GetInt64(5));
    }

    /// <summary>
    /// P0-6：在读取 messages（FOR UPDATE）之前，确保 message_state 行存在并持有其 FOR UPDATE 锁。
    /// <para>
    /// 锁顺序统一为 message_state → messages：Recall 与 Reaction 均先锁 message_state 再访问 messages，
    /// 消除"Recall 持 messages 锁等 message_state，Reaction 持 message_state 锁等 messages"的死锁。
    /// </para>
    /// </summary>
    public async Task EnsureMessageStateLockedAsync(string messageId)
    {
        var ct = _session.CancellationToken;
        // 先确保状态行存在（新消息可能尚无 message_state 行）
        await using (var ensureCmd = new NpgsqlCommand(
                        $"""
                        INSERT INTO {_session.Schema.MessageStateTableSql} ("message_id", "changed_at_ms")
                        VALUES (@message_id, 0)
                        ON CONFLICT ("message_id") DO NOTHING
                        """,
                        _session.Connection,
                        _session.Transaction))
        {
            ensureCmd.Parameters.AddWithValue("message_id", messageId);
            await ensureCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 锁定 message_state 行，阻塞 Reaction 的 SELECT FOR UPDATE
        await using (var lockCmd = new NpgsqlCommand(
                        $"""
                        SELECT 1 FROM {_session.Schema.MessageStateTableSql}
                        WHERE message_id = @message_id
                        FOR UPDATE
                        """,
                        _session.Connection,
                        _session.Transaction))
        {
            lockCmd.Parameters.AddWithValue("message_id", messageId);
            await using var lockReader = await lockCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await lockReader.ReadAsync(ct).ConfigureAwait(false);
        }
    }

    public async Task<int> ApplyRecallUpdateAsync(string messageId, long recalledAtMs)
    {
        var ct = _session.CancellationToken;
        // P0-6：同步更新 message_state.recalled_at_ms，使 Reaction 在持有 message_state FOR UPDATE 锁后
        // 能读到一致的撤回状态，消除"无锁读 messages → 锁 message_state"两步间的撤回竞态。
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
               AND recalled_at_ms IS NULL;
             UPDATE {_session.Schema.MessageStateTableSql}
             SET "recalled_at_ms" = @recalled_at_ms
             WHERE message_id = @message_id
               AND "recalled_at_ms" IS NULL;
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
                    recalled_at_ms, content, edit_version, mentioned_user_ids, mentioned_roles,
                    conversation_sequence
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
            reader.GetInt32(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<long[]>(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<string[]>(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9));
    }

    public async Task<EditUpdateResult> ApplyEditUpdateAsync(
        string messageId,
        string content,
        int editVersion,
        long editedAtMs,
        long[]? mentionedUserIds,
        string[]? mentionedRoles,
        long[]? previousMentionedUserIds)
    {
        var ct = _session.CancellationToken;
        // mentions 为 null 时保留原值（COALESCE 语义：NULL 输入 → 不修改该列）；
        // 非空数组（包括空数组）替换原值。
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_session.Schema.MessagesTableSql}
             SET content = @content,
                 edit_version = @edit_version,
                 edited_at_ms = @edited_at_ms,
                 changed_at_ms = @edited_at_ms,
                 mentioned_user_ids = COALESCE(@mentioned_user_ids, mentioned_user_ids),
                 mentioned_roles = COALESCE(@mentioned_roles, mentioned_roles)
             WHERE message_id = @message_id
               AND recalled_at_ms IS NULL
             """,
            _session.Connection,
            _session.Transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("content", content);
        command.Parameters.AddWithValue("edit_version", editVersion);
        command.Parameters.AddWithValue("edited_at_ms", editedAtMs);
        // Npgsql：空数组需写为 DBNull 才能置 NULL；非空数组直接传入。
        // MentionValidator.AsReadOnly 已经把"全部过滤掉"的 mention 规整为 null，
        // 但编辑路径调用方需区分"不修改（null）"与"清空（empty）"。
        // 此处 long[]?/string[]? 为 null → COALESCE 保留原值；为空数组 → 置 NULL（语义为无 mention）。
        command.Parameters.AddWithValue(
            "mentioned_user_ids",
            mentionedUserIds is null
                ? DBNull.Value
                : mentionedUserIds.Length == 0
                    ? DBNull.Value
                    : mentionedUserIds);
        command.Parameters.AddWithValue(
            "mentioned_roles",
            mentionedRoles is null
                ? DBNull.Value
                : mentionedRoles.Length == 0
                    ? DBNull.Value
                    : mentionedRoles);
        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // 一-4：计算 mention 增减 diff，供 Edit 结果与事件 payload 携带。
        // mentionedUserIds 为 null（未修改 mentions）时 diff 为 null；
        // 非 null（含空数组，表示清空）时与 previousMentionedUserIds 求差集。
        var (addedMentionedUserIds, removedMentionedUserIds) = ComputeMentionDiff(
            previousMentionedUserIds, mentionedUserIds);
        return new EditUpdateResult(affected, addedMentionedUserIds, removedMentionedUserIds);
    }

    /// <summary>
    /// 一-4：计算 Edit 操作的 mention 增减 diff。
    /// </summary>
    /// <param name="previous">编辑前的 mentioned_user_ids（来自 ReadMessageForEditAsync）。</param>
    /// <param name="current">编辑写入的新 mentioned_user_ids；<c>null</c> 表示未修改 mentions。</param>
    /// <returns>
    /// added/removed：当 <paramref name="current"/> 为 <c>null</c>（未修改）时均为 <c>null</c>；
    /// 否则为非空列表（可能为空列表，表示无新增 / 无移除）。
    /// </returns>
    internal static (IReadOnlyList<long>? Added, IReadOnlyList<long>? Removed) ComputeMentionDiff(
        long[]? previous,
        long[]? current)
    {
        if (current is null)
        {
            return (null, null);
        }

        var previousSet = previous is null || previous.Length == 0
            ? new HashSet<long>()
            : new HashSet<long>(previous);
        var currentSet = new HashSet<long>(current);

        var added = currentSet.Except(previousSet).ToArray();
        var removed = previousSet.Except(currentSet).ToArray();

        return (added, removed);
    }

    public static string ComputeMutationFingerprint(
        short operation,
        string messageId,
        string content,
        IReadOnlyList<long>? mentionedUserIds = null,
        IReadOnlyList<string>? mentionedRoles = null)
    {
        // 编辑指纹纳入 mentions，保证"同 RequestId 不同 mentions"被判为 RequestConflict；
        // 撤回 operation=2 不传 mentions，null → "null" 哨兵，与历史 v1 指纹不同，
        // 但本仓所有 mutation_requests 行均按当前代码版本写入，不存在跨版本比对。
        var usersPart = mentionedUserIds is null
            ? "null"
            : string.Join(",", mentionedUserIds);
        var rolesPart = mentionedRoles is null
            ? "null"
            : string.Join(",", mentionedRoles);
        var input = System.Text.Encoding.UTF8.GetBytes(
            $"{operation}\n{messageId}\n{content}\n{usersPart}\n{rolesPart}");
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
        long? RecalledAtMs,
        long? ConversationSequence);

    public sealed record EditTargetRow(
        long SenderUserId,
        long ReceiverUserId,
        string? ConversationId,
        long ReceivedAtMs,
        long? RecalledAtMs,
        string Content,
        int EditVersion,
        long[]? MentionedUserIds,
        string[]? MentionedRoles,
        long? ConversationSequence);

    /// <summary>
    /// 一-4：ApplyEditUpdateAsync 的返回结果，包含受影响行数与 mention 增减 diff。
    /// </summary>
    public sealed record EditUpdateResult(
        int Affected,
        IReadOnlyList<long>? AddedMentionedUserIds,
        IReadOnlyList<long>? RemovedMentionedUserIds);
}

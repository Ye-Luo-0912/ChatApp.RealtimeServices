using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly ILogger<NpgsqlRealtimeMessageStore> _logger;

    public NpgsqlRealtimeMessageStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        ILogger<NpgsqlRealtimeMessageStore> logger)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _logger = logger;
    }

    public async Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        var fingerprint = RealtimeMessageFingerprint.Compute(
            message.ReceiverUserId,
            message.Content,
            message.AttachmentIds);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_databaseSchema.MessagesTableSql} (
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
                1,
                @received_at_ms
            )
            ON CONFLICT (sender_user_id, client_message_id) DO NOTHING;
            """,
            connection,
            transaction);

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

        var affectedRows = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (affectedRows == 0)
        {
            var existing = await GetExistingMessageAsync(connection, transaction, message, ct)
                .ConfigureAwait(false);
            var existingAttachmentIds = await ListAttachmentIdsForMessageAsync(
                    connection,
                    transaction,
                    existing.MessageId,
                    ct)
                .ConfigureAwait(false);
            if (!RealtimeMessageFingerprint.MatchesExisting(
                    existing.Fingerprint,
                    existing.ReceiverUserId,
                    existing.Content,
                    existingAttachmentIds,
                    fingerprint))
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                _logger.LogWarning(
                    "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                    message.ClientMessageId,
                    message.SenderUserId,
                    existing.MessageId);
                return RealtimeMessagePersistResult.Conflict(existing.MessageId);
            }

            // 消息与 Outbox 同事务提交：重复投递不重建 Published/Dead（及清理后的）Outbox 行。
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            _logger.LogDebug(
                "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return RealtimeMessagePersistResult.Duplicate(existing.MessageId);
        }

        IReadOnlyList<AttachmentRef>? boundAttachmentRefs = null;
        if (message.AttachmentIds is { Count: > 0 })
        {
            var expected = message.AttachmentIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.Ordinal)
                .Count();
            try
            {
                var boundRecords = await AttachmentWriteCommands.BindConfirmedToMessageAsync(
                        connection,
                        transaction,
                        _databaseSchema,
                        message.MessageId,
                        message.ConversationId,
                        message.SenderUserId,
                        message.AttachmentIds,
                        ct)
                    .ConfigureAwait(false);
                if (boundRecords.Count != expected)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    _logger.LogWarning(
                        "附件绑定失败。消息={MessageId}；期望={Expected}；实际={Bound}",
                        message.MessageId,
                        expected,
                        boundRecords.Count);
                    return RealtimeMessagePersistResult.AttachmentBindFailed(message.MessageId);
                }

                boundAttachmentRefs = AttachmentRefMapper.FromRecords(boundRecords);
            }
            catch (InvalidOperationException ex)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                _logger.LogWarning(
                    ex,
                    "附件绑定拒绝。消息={MessageId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.AttachmentBindFailed(message.MessageId);
            }
        }

        var createdEvent = EnrichChatMessagePayload(
            CopyWithMessageId(eventToPublish, message.MessageId),
            boundAttachmentRefs);
        var outboxEvents = new List<RealtimeEvent>(8);

        var isGroup = !string.IsNullOrWhiteSpace(message.ConversationId)
                      && ConversationId.IsGroup(message.ConversationId);

        if (isGroup)
        {
            var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                    connection,
                    transaction,
                    _databaseSchema,
                    message.ConversationId!,
                    ct)
                .ConfigureAwait(false);

            if (memberIds.Count == 0 || !memberIds.Contains(message.SenderUserId))
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                _logger.LogWarning(
                    "群消息写入拒绝：发送方不是成员。消息={MessageId}；会话={ConversationId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.ConversationId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.NotAllowed(message.MessageId);
            }

            foreach (var targetUserId in memberIds)
            {
                outboxEvents.Add(CreateGroupMessageReceivedEvent(
                    createdEvent,
                    message,
                    targetUserId));
            }

            await AdvanceGroupConversationAndEnqueueAsync(
                    connection,
                    transaction,
                    message,
                    memberIds,
                    createdEvent.TraceParent,
                    createdEvent.TraceState,
                    outboxEvents,
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            outboxEvents.Add(createdEvent);

            // 发送方其他在线设备回声：同事务写入；Gateway 会跳过来源 SessionId。
            if (message.SenderUserId != message.ReceiverUserId)
                outboxEvents.Add(CreateSenderEchoEvent(createdEvent, message.SenderUserId));

            if (!string.IsNullOrWhiteSpace(message.ConversationId))
            {
                await AdvanceConversationAndEnqueueAsync(
                        connection,
                        transaction,
                        message,
                        createdEvent.TraceParent,
                        createdEvent.TraceState,
                        outboxEvents,
                        ct)
                    .ConfigureAwait(false);
            }
        }

        await InsertOutboxManyAsync(connection, transaction, outboxEvents, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        _logger.LogDebug(
            "实时消息已通过 Npgsql 写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);
        return RealtimeMessagePersistResult.Created(message.MessageId);
    }

    private async Task AdvanceGroupConversationAndEnqueueAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeMessageRecord message,
        IReadOnlyList<long> memberIds,
        string? traceParent,
        string? traceState,
        List<RealtimeEvent> outboxEvents,
        CancellationToken ct)
    {
        var conversationId = message.ConversationId!;
        var preview = ConversationId.CreatePreview(message.Content);

        var (advanced, unreads) = await ConversationWriteCommands.TryAdvanceGroupAndIncrementUnreadAsync(
                connection,
                transaction,
                _databaseSchema,
                conversationId,
                message.SenderUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs,
                ct)
            .ConfigureAwait(false);

        if (advanced)
        {
            foreach (var memberId in memberIds)
            {
                outboxEvents.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    memberId,
                    peerUserId: null,
                    message.MessageId,
                    preview,
                    message.ReceivedAtMs,
                    message.SenderUserId,
                    traceParent,
                    traceState,
                    type: ConversationType.Group));
            }
        }

        foreach (var (userId, unreadCount) in unreads)
        {
            outboxEvents.Add(ConversationWriteCommands.CreateUnreadCountChangedEvent(
                conversationId,
                userId,
                unreadCount,
                lastReadMessageId: null,
                lastReadAtMs: null,
                causeMessageId: message.MessageId,
                message.ReceivedAtMs,
                traceParent,
                traceState));
        }
    }

    private static RealtimeEvent CreateGroupMessageReceivedEvent(
        RealtimeEvent template,
        RealtimeMessageRecord message,
        long targetUserId)
    {
        return new RealtimeEvent
        {
            EventId = RealtimeEventContracts.CreateMessageReceivedEventId(
                message.SenderUserId,
                message.ClientMessageId,
                targetUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = targetUserId,
            ActorUserId = message.SenderUserId,
            MessageId = message.MessageId,
            SessionId = message.SenderSessionId,
            PayloadJson = template.PayloadJson,
            OccurredAtMs = template.OccurredAtMs,
            TraceParent = template.TraceParent,
            TraceState = template.TraceState
        };
    }

    private async Task AdvanceConversationAndEnqueueAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeMessageRecord message,
        string? traceParent,
        string? traceState,
        List<RealtimeEvent> outboxEvents,
        CancellationToken ct)
    {
        var conversationId = message.ConversationId!;
        var preview = ConversationId.CreatePreview(message.Content);

        var (advanced, unread) = await ConversationWriteCommands.TryAdvanceAndIncrementUnreadAsync(
                connection,
                transaction,
                _databaseSchema,
                conversationId,
                message.SenderUserId,
                message.ReceiverUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs,
                ct)
            .ConfigureAwait(false);

        if (advanced)
        {
            outboxEvents.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                conversationId,
                message.SenderUserId,
                message.ReceiverUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs,
                message.SenderUserId,
                traceParent,
                traceState));
            outboxEvents.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                conversationId,
                message.ReceiverUserId,
                message.SenderUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs,
                message.SenderUserId,
                traceParent,
                traceState));
        }

        if (unread is int unreadCount)
        {
            outboxEvents.Add(ConversationWriteCommands.CreateUnreadCountChangedEvent(
                conversationId,
                message.ReceiverUserId,
                unreadCount,
                lastReadMessageId: null,
                lastReadAtMs: null,
                causeMessageId: message.MessageId,
                message.ReceivedAtMs,
                traceParent,
                traceState));
        }
    }

    public async Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        long senderUserId;
        long receiverUserId;
        long? deliveredAtMs;
        long? readAtMs;
        string? conversationId;
        long messageReceivedAtMs;

        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, delivered_at_ms, read_at_ms,
                                 conversation_id, received_at_ms
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            await using var reader = await command
                .ExecuteReaderAsync(ct)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.MessageNotFound,
                    receipt.MessageId);
            }

            senderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            deliveredAtMs = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            readAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
            conversationId = reader.IsDBNull(4) ? null : reader.GetString(4);
            messageReceivedAtMs = reader.GetInt64(5);
        }

        if (receiverUserId != receipt.ReceiverUserId)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.ReceiverMismatch,
                receipt.MessageId,
                senderUserId);
        }

        var shouldApply = receipt.ReceiptType switch
        {
            MessageReceiptType.Delivered => deliveredAtMs is null && readAtMs is null,
            MessageReceiptType.Read => readAtMs is null,
            _ => false
        };
        if (!shouldApply)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.Unchanged,
                receipt.MessageId,
                senderUserId);
        }

        var setClause = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms = @occurred_at_ms, delivered_at_ms = COALESCE(delivered_at_ms, @occurred_at_ms)"
            : "delivered_at_ms = @occurred_at_ms";
        var condition = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms IS NULL"
            : "delivered_at_ms IS NULL AND read_at_ms IS NULL";

        await using (var command = new NpgsqlCommand(
                         $"UPDATE {_databaseSchema.MessagesTableSql} SET {setClause} WHERE message_id = @message_id AND receiver_user_id = @receiver_user_id AND {condition}",
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            command.Parameters.AddWithValue("receiver_user_id", receipt.ReceiverUserId);
            command.Parameters.AddWithValue("occurred_at_ms", receipt.OccurredAtMs);
            var affectedRows = await command
                .ExecuteNonQueryAsync(ct)
                .ConfigureAwait(false);
            if (affectedRows == 0)
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId,
                    senderUserId);
            }
        }

        await InsertOutboxAsync(
                connection,
                transaction,
                CopyForReceipt(eventToPublish, senderUserId),
                ct)
            .ConfigureAwait(false);

        if (receipt.ReceiptType == MessageReceiptType.Read
            && !string.IsNullOrWhiteSpace(conversationId))
        {
            await AdvanceConversationReadInTransactionAsync(
                    connection,
                    transaction,
                    receipt.ReceiverUserId,
                    conversationId,
                    messageReceivedAtMs,
                    receipt.MessageId,
                    ct)
                .ConfigureAwait(false);
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);

        return new MessageReceiptPersistResult(
            MessageReceiptPersistStatus.Applied,
            receipt.MessageId,
            senderUserId);
    }

    private async Task AdvanceConversationReadInTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long userId,
        string conversationId,
        long readAtMs,
        string readMessageId,
        CancellationToken ct)
    {
        long? currentReadAtMs;
        string? currentReadMessageId;

        await using (var load = new NpgsqlCommand(
                           $"""
                            SELECT last_read_at_ms, last_read_message_id
                            FROM {_databaseSchema.ConversationMembersTableSql}
                            WHERE conversation_id = @conversation_id
                              AND user_id = @user_id
                            FOR UPDATE;
                            """,
                           connection,
                           transaction))
        {
            load.Parameters.AddWithValue("conversation_id", conversationId);
            load.Parameters.AddWithValue("user_id", userId);
            await using var reader = await load.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return;

            currentReadAtMs = reader.IsDBNull(0) ? null : reader.GetInt64(0);
            currentReadMessageId = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        var shouldAdvance = currentReadAtMs is null
            || currentReadAtMs.Value < readAtMs
            || (currentReadAtMs.Value == readAtMs
                && (currentReadMessageId is null
                    || string.CompareOrdinal(currentReadMessageId, readMessageId) < 0));
        if (!shouldAdvance)
            return;

        await using var countCmd = new NpgsqlCommand(
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
            update.Parameters.AddWithValue("read_at_ms", readAtMs);
            update.Parameters.AddWithValue("read_message_id", readMessageId);
            update.Parameters.AddWithValue("unread_count", unread);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await InsertOutboxAsync(
                connection,
                transaction,
                ConversationWriteCommands.CreateUnreadCountChangedEvent(
                    conversationId,
                    userId,
                    unread,
                    readMessageId,
                    readAtMs,
                    causeMessageId: readMessageId,
                    readAtMs,
                    null,
                    null),
                ct)
            .ConfigureAwait(false);
    }

    private async Task<(string MessageId, long ReceiverUserId, string Content, string? Fingerprint)> GetExistingMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeMessageRecord message,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT message_id, receiver_user_id, content, content_fingerprint
             FROM {_databaseSchema.MessagesTableSql}
             WHERE sender_user_id = @sender_user_id AND client_message_id = @client_message_id
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("sender_user_id", message.SenderUserId);
        command.Parameters.AddWithValue("client_message_id", message.ClientMessageId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException("检测到消息冲突，但无法读取已有消息编号。");

        return (
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private async Task<IReadOnlyList<string>> ListAttachmentIdsForMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id
             FROM {_databaseSchema.AttachmentsTableSql}
             WHERE message_id = @message_id
             ORDER BY attachment_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        var ids = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            ids.Add(reader.GetString(0));
        return ids;
    }

    private static RealtimeEvent EnrichChatMessagePayload(
        RealtimeEvent evt,
        IReadOnlyList<AttachmentRef>? attachments)
    {
        if (string.IsNullOrWhiteSpace(evt.PayloadJson))
            return evt;

        RealtimeChatMessagePayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize(
                evt.PayloadJson,
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
        }
        catch (JsonException)
        {
            return evt;
        }

        if (payload is null)
            return evt;

        var enriched = new RealtimeChatMessagePayload
        {
            PayloadVersion = RealtimeChatMessagePayload.CurrentPayloadVersion,
            MessageId = payload.MessageId,
            ClientMessageId = payload.ClientMessageId,
            SenderUserId = payload.SenderUserId,
            SenderSessionId = payload.SenderSessionId,
            ReceiverUserId = payload.ReceiverUserId,
            Content = payload.Content,
            ConversationId = payload.ConversationId,
            ReceivedAtMs = payload.ReceivedAtMs,
            Attachments = attachments is { Count: > 0 } ? attachments : payload.Attachments,
            ReplyToMessageId = payload.ReplyToMessageId,
            ReplyToSenderUserId = payload.ReplyToSenderUserId,
            ReplyToPreview = payload.ReplyToPreview,
            ForwardedFromMessageId = payload.ForwardedFromMessageId,
            ForwardedFromSenderUserId = payload.ForwardedFromSenderUserId,
            ForwardedFromPreview = payload.ForwardedFromPreview
        };

        return new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = JsonSerializer.Serialize(
                enriched,
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };
    }

    private async Task InsertOutboxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeEvent evt,
        CancellationToken ct) =>
        await InsertOutboxManyAsync(connection, transaction, [evt], ct).ConfigureAwait(false);

    private async Task InsertOutboxManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<RealtimeEvent> events,
        CancellationToken ct)
    {
        if (events.Count == 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var values = new List<string>(events.Count);
        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            values.Add(
                $"(@event_id_{i}, @payload_json_{i}, @target_user_id_{i}, @event_type_{i}, @status, @created_at_ms, @next_attempt_at_ms, 0)");
            command.Parameters.AddWithValue($"event_id_{i}", evt.EventId);
            command.Parameters.AddWithValue(
                $"payload_json_{i}",
                JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
            command.Parameters.AddWithValue($"target_user_id_{i}", evt.TargetUserId);
            command.Parameters.AddWithValue($"event_type_{i}", (short)evt.Type);
        }

        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        command.CommandText =
            $"""
             INSERT INTO {_databaseSchema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES
                 {string.Join(",\n                 ", values)}
             ON CONFLICT (event_id) DO NOTHING;
             """;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static RealtimeEvent CopyForReceipt(
        RealtimeEvent evt,
        long senderUserId) => new()
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = senderUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };

    private static RealtimeEvent CopyWithMessageId(RealtimeEvent evt, string messageId) => new()
    {
        EventId = evt.EventId,
        Type = evt.Type,
        TargetUserId = evt.TargetUserId,
        ActorUserId = evt.ActorUserId,
        MessageId = messageId,
        SessionId = evt.SessionId,
        PayloadJson = evt.PayloadJson,
        OccurredAtMs = evt.OccurredAtMs,
        TraceParent = evt.TraceParent,
        TraceState = evt.TraceState
    };

    private static RealtimeEvent CreateSenderEchoEvent(RealtimeEvent receiverEvent, long senderUserId)
    {
        var messageId = receiverEvent.MessageId ?? string.Empty;
        return new RealtimeEvent
        {
            EventId = RealtimeEventContracts.CreateSenderEchoEventId(messageId, senderUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = senderUserId,
            ActorUserId = receiverEvent.ActorUserId,
            MessageId = receiverEvent.MessageId,
            SessionId = receiverEvent.SessionId,
            PayloadJson = receiverEvent.PayloadJson,
            OccurredAtMs = receiverEvent.OccurredAtMs,
            TraceParent = receiverEvent.TraceParent,
            TraceState = receiverEvent.TraceState
        };
    }

    public async Task<MessageRecallPersistResult> ApplyRecallAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        long recalledAtMs,
        long maxAgeMs,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(senderUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recalledAtMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAgeMs);

        var payloadFingerprint = ComputeMutationFingerprint(
            operation: 2,
            messageId,
            content: string.Empty);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var prior = await TryReadMutationRequestAsync(
                connection,
                transaction,
                senderUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 2
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return new MessageRecallPersistResult(
                    MessageRecallPersistStatus.RequestConflict,
                    messageId);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageRecallPersistResult(
                    MessageRecallPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    RecalledAtMs: prior.RecalledAtMs)
                : MapRecallFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        long dbSenderUserId;
        long receiverUserId;
        string? conversationId;
        long receivedAtMs;
        long? existingRecalledAtMs;

        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, conversation_id, received_at_ms, recalled_at_ms
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            await using var reader = await command
                .ExecuteReaderAsync(ct)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                await InsertMutationRequestAsync(
                        connection,
                        transaction,
                        senderUserId,
                        requestId,
                        operation: 2,
                        messageId,
                        payloadFingerprint,
                        succeeded: false,
                        errorCode: "message_not_found",
                        conversationId: null,
                        content: null,
                        editVersion: null,
                        editedAtMs: null,
                        recalledAtMs: null,
                        ct)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageRecallPersistResult(
                    MessageRecallPersistStatus.NotFound,
                    messageId);
            }

            dbSenderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            conversationId = reader.IsDBNull(2) ? null : reader.GetString(2);
            receivedAtMs = reader.GetInt64(3);
            existingRecalledAtMs = reader.IsDBNull(4) ? null : reader.GetInt64(4);
        }

        if (dbSenderUserId != senderUserId)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_not_allowed",
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                receiverUserId,
                conversationId);
        }

        if (existingRecalledAtMs is long already)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: already,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.Unchanged,
                messageId,
                receiverUserId,
                conversationId,
                already);
        }

        if (recalledAtMs - receivedAtMs > maxAgeMs)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_window_expired",
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.WindowExpired,
                messageId,
                receiverUserId,
                conversationId);
        }

        await using (var command = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.MessagesTableSql}
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
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            command.Parameters.AddWithValue("recalled_at_ms", recalledAtMs);
            var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (affected == 0)
            {
                await InsertMutationRequestAsync(
                        connection,
                        transaction,
                        senderUserId,
                        requestId,
                        operation: 2,
                        messageId,
                        payloadFingerprint,
                        succeeded: true,
                        errorCode: null,
                        conversationId,
                        content: null,
                        editVersion: null,
                        editedAtMs: null,
                        recalledAtMs: recalledAtMs,
                        ct)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageRecallPersistResult(
                    MessageRecallPersistStatus.Unchanged,
                    messageId,
                    receiverUserId,
                    conversationId,
                    recalledAtMs);
            }
        }

        var tipPreviewUpdated = false;
        if (!string.IsNullOrWhiteSpace(conversationId))
        {
            await using var previewCmd = new NpgsqlCommand(
                $"""
                 UPDATE {_databaseSchema.ConversationsTableSql}
                 SET last_message_preview = @preview
                 WHERE conversation_id = @conversation_id
                   AND last_message_id = @message_id
                 """,
                connection,
                transaction);
            previewCmd.Parameters.AddWithValue("preview", "消息已撤回");
            previewCmd.Parameters.AddWithValue("conversation_id", conversationId);
            previewCmd.Parameters.AddWithValue("message_id", messageId);
            tipPreviewUpdated = await previewCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false) > 0;
        }

        var payloadJson = JsonSerializer.Serialize(
            new RealtimeMessageRecalledPayload
            {
                MessageId = messageId,
                ConversationId = conversationId,
                SenderUserId = senderUserId,
                ReceiverUserId = receiverUserId,
                RecalledAtMs = recalledAtMs
            },
            RealtimeJsonSerializerContext.Default.RealtimeMessageRecalledPayload);

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var events = new List<RealtimeEvent>(8);
        var isGroup = !string.IsNullOrWhiteSpace(conversationId)
                      && ConversationId.IsGroup(conversationId);

        if (isGroup)
        {
            var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                    connection,
                    transaction,
                    _databaseSchema,
                    conversationId!,
                    ct)
                .ConfigureAwait(false);
            foreach (var targetUserId in memberIds)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = RealtimeEventContracts.CreateMessageRecalledEventId(messageId, targetUserId),
                    Type = RealtimeEventType.MessageRecalled,
                    TargetUserId = targetUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = recalledAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated)
            {
                var cause = $"recall:{recalledAtMs}";
                foreach (var targetUserId in memberIds)
                {
                    events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                        conversationId!,
                        targetUserId,
                        peerUserId: null,
                        messageId,
                        "消息已撤回",
                        receivedAtMs,
                        senderUserId,
                        traceParent,
                        traceState,
                        cause,
                        ConversationType.Group));
                }
            }
        }
        else
        {
            events.Add(new RealtimeEvent
            {
                EventId = RealtimeEventContracts.CreateMessageRecalledEventId(messageId, receiverUserId),
                Type = RealtimeEventType.MessageRecalled,
                TargetUserId = receiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = recalledAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != receiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = RealtimeEventContracts.CreateMessageRecalledEventId(messageId, senderUserId),
                    Type = RealtimeEventType.MessageRecalled,
                    TargetUserId = senderUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = recalledAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(conversationId))
            {
                var cause = $"recall:{recalledAtMs}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    senderUserId,
                    receiverUserId,
                    messageId,
                    "消息已撤回",
                    receivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    receiverUserId,
                    senderUserId,
                    messageId,
                    "消息已撤回",
                    receivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        await InsertMutationRequestAsync(
                connection,
                transaction,
                senderUserId,
                requestId,
                operation: 2,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                conversationId,
                content: null,
                editVersion: null,
                editedAtMs: null,
                recalledAtMs: recalledAtMs,
                ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        return new MessageRecallPersistResult(
            MessageRecallPersistStatus.Applied,
            messageId,
            receiverUserId,
            conversationId,
            recalledAtMs);
    }

    public async Task<MessageEditPersistResult> ApplyEditAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        string content,
        long editedAtMs,
        long maxAgeMs,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentNullException.ThrowIfNull(content);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(senderUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(editedAtMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAgeMs);

        var payloadFingerprint = ComputeMutationFingerprint(
            operation: 1,
            messageId,
            content);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var prior = await TryReadMutationRequestAsync(
                connection,
                transaction,
                senderUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 1
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return new MessageEditPersistResult(
                    MessageEditPersistStatus.RequestConflict,
                    messageId);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageEditPersistResult(
                    MessageEditPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    Content: prior.Content,
                    EditVersion: prior.EditVersion,
                    EditedAtMs: prior.EditedAtMs)
                : MapEditFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        long dbSenderUserId;
        long receiverUserId;
        string? conversationId;
        long receivedAtMs;
        long? recalledAtMs;
        string existingContent;
        int editVersion;

        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, conversation_id, received_at_ms,
                                 recalled_at_ms, content, edit_version
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            await using var reader = await command
                .ExecuteReaderAsync(ct)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                await InsertMutationRequestAsync(
                        connection,
                        transaction,
                        senderUserId,
                        requestId,
                        operation: 1,
                        messageId,
                        payloadFingerprint,
                        succeeded: false,
                        errorCode: "message_not_found",
                        conversationId: null,
                        content: null,
                        editVersion: null,
                        editedAtMs: null,
                        recalledAtMs: null,
                        ct)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageEditPersistResult(
                    MessageEditPersistStatus.NotFound,
                    messageId);
            }

            dbSenderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            conversationId = reader.IsDBNull(2) ? null : reader.GetString(2);
            receivedAtMs = reader.GetInt64(3);
            recalledAtMs = reader.IsDBNull(4) ? null : reader.GetInt64(4);
            existingContent = reader.GetString(5);
            editVersion = reader.GetInt32(6);
        }

        if (dbSenderUserId != senderUserId)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_not_allowed",
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                receiverUserId,
                conversationId);
        }

        if (recalledAtMs is not null)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_recalled",
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: recalledAtMs,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                receiverUserId,
                conversationId);
        }

        if (editedAtMs - receivedAtMs > maxAgeMs)
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_window_expired",
                    conversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.WindowExpired,
                messageId,
                receiverUserId,
                conversationId);
        }

        if (string.Equals(existingContent, content, StringComparison.Ordinal))
        {
            await InsertMutationRequestAsync(
                    connection,
                    transaction,
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    conversationId,
                    content: existingContent,
                    editVersion: editVersion,
                    editedAtMs: editedAtMs,
                    recalledAtMs: null,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.Unchanged,
                messageId,
                receiverUserId,
                conversationId,
                existingContent,
                editVersion,
                editedAtMs);
        }

        var nextVersion = editVersion + 1;
        await using (var command = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.MessagesTableSql}
                          SET content = @content,
                              edit_version = @edit_version,
                              edited_at_ms = @edited_at_ms,
                              changed_at_ms = @edited_at_ms
                          WHERE message_id = @message_id
                            AND recalled_at_ms IS NULL
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            command.Parameters.AddWithValue("content", content);
            command.Parameters.AddWithValue("edit_version", nextVersion);
            command.Parameters.AddWithValue("edited_at_ms", editedAtMs);
            var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (affected == 0)
            {
                await InsertMutationRequestAsync(
                        connection,
                        transaction,
                        senderUserId,
                        requestId,
                        operation: 1,
                        messageId,
                        payloadFingerprint,
                        succeeded: false,
                        errorCode: "message_recalled",
                        conversationId,
                        content: null,
                        editVersion: null,
                        editedAtMs: null,
                        recalledAtMs: null,
                        ct)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageEditPersistResult(
                    MessageEditPersistStatus.AlreadyRecalled,
                    messageId,
                    receiverUserId,
                    conversationId);
            }
        }

        var tipPreviewUpdated = false;
        var tipPreview = ConversationId.CreatePreview(content);
        if (!string.IsNullOrWhiteSpace(conversationId))
        {
            await using var previewCmd = new NpgsqlCommand(
                $"""
                 UPDATE {_databaseSchema.ConversationsTableSql}
                 SET last_message_preview = @preview
                 WHERE conversation_id = @conversation_id
                   AND last_message_id = @message_id
                 """,
                connection,
                transaction);
            previewCmd.Parameters.AddWithValue("preview", tipPreview);
            previewCmd.Parameters.AddWithValue("conversation_id", conversationId);
            previewCmd.Parameters.AddWithValue("message_id", messageId);
            tipPreviewUpdated = await previewCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false) > 0;
        }

        var payloadJson = JsonSerializer.Serialize(
            new RealtimeMessageEditedPayload
            {
                MessageId = messageId,
                ConversationId = conversationId,
                SenderUserId = senderUserId,
                ReceiverUserId = receiverUserId,
                Content = content,
                EditVersion = nextVersion,
                EditedAtMs = editedAtMs
            },
            RealtimeJsonSerializerContext.Default.RealtimeMessageEditedPayload);

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var events = new List<RealtimeEvent>(8);
        var isGroup = !string.IsNullOrWhiteSpace(conversationId)
                      && ConversationId.IsGroup(conversationId);

        if (isGroup)
        {
            var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                    connection,
                    transaction,
                    _databaseSchema,
                    conversationId!,
                    ct)
                .ConfigureAwait(false);
            foreach (var targetUserId in memberIds)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = RealtimeEventContracts.CreateMessageEditedEventId(
                        messageId,
                        targetUserId,
                        nextVersion),
                    Type = RealtimeEventType.MessageEdited,
                    TargetUserId = targetUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = editedAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated)
            {
                var cause = $"edit:{nextVersion}";
                foreach (var targetUserId in memberIds)
                {
                    events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                        conversationId!,
                        targetUserId,
                        peerUserId: null,
                        messageId,
                        tipPreview,
                        receivedAtMs,
                        senderUserId,
                        traceParent,
                        traceState,
                        cause,
                        ConversationType.Group));
                }
            }
        }
        else
        {
            events.Add(new RealtimeEvent
            {
                EventId = RealtimeEventContracts.CreateMessageEditedEventId(
                    messageId,
                    receiverUserId,
                    nextVersion),
                Type = RealtimeEventType.MessageEdited,
                TargetUserId = receiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = editedAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != receiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = RealtimeEventContracts.CreateMessageEditedEventId(
                        messageId,
                        senderUserId,
                        nextVersion),
                    Type = RealtimeEventType.MessageEdited,
                    TargetUserId = senderUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = editedAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(conversationId))
            {
                var cause = $"edit:{nextVersion}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    senderUserId,
                    receiverUserId,
                    messageId,
                    tipPreview,
                    receivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    receiverUserId,
                    senderUserId,
                    messageId,
                    tipPreview,
                    receivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        await InsertMutationRequestAsync(
                connection,
                transaction,
                senderUserId,
                requestId,
                operation: 1,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                conversationId,
                content: content,
                editVersion: nextVersion,
                editedAtMs: editedAtMs,
                recalledAtMs: null,
                ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        return new MessageEditPersistResult(
            MessageEditPersistStatus.Applied,
            messageId,
            receiverUserId,
            conversationId,
            content,
            nextVersion,
            editedAtMs);
    }

    private static MessageRecallPersistResult MapRecallFailure(
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

    private static MessageEditPersistResult MapEditFailure(
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

    private static string ComputeMutationFingerprint(
        short operation,
        string messageId,
        string content)
    {
        var input = System.Text.Encoding.UTF8.GetBytes($"{operation}\n{messageId}\n{content}");
        return Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(input));
    }

    private sealed record MutationRequestRow(
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

    private async Task<MutationRequestRow?> TryReadMutationRequestAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long actorUserId,
        string requestId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT operation, message_id, payload_fingerprint, succeeded, error_code,
                    conversation_id, content, edit_version, edited_at_ms, recalled_at_ms
             FROM {_databaseSchema.MessageMutationRequestsTableSql}
             WHERE actor_user_id = @actor_user_id
               AND request_id = @request_id
             FOR UPDATE
             """,
            connection,
            transaction);
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

    private async Task InsertMutationRequestAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
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
        long? recalledAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.MessageMutationRequestsTableSql} (
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
            connection,
            transaction);
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

    public async Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 5_000);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        long total = 0;
        while (true)
        {
            await using var command = new NpgsqlCommand(
                $"""
                 DELETE FROM {_databaseSchema.MessagesTableSql}
                 WHERE ctid IN (
                     SELECT ctid FROM {_databaseSchema.MessagesTableSql}
                     WHERE sender_user_id = @user_id OR receiver_user_id = @user_id
                     LIMIT @batch_size
                 );
                 """,
                connection);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("batch_size", batchSize);
            var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (deleted <= 0)
                break;
            total += deleted;
        }

        await DeleteConversationDataForUserIfPresentAsync(connection, userId, ct)
            .ConfigureAwait(false);

        await using (var outboxCmd = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE target_user_id = @user_id
               AND event_type <> ALL(@keep_types);
             """,
            connection))
        {
            outboxCmd.Parameters.AddWithValue("user_id", userId);
            outboxCmd.Parameters.AddWithValue(
                "keep_types",
                new short[]
                {
                    (short)RealtimeEventType.AccountCleanupCompleted,
                    (short)RealtimeEventType.AttachmentBlobsPurge
                });
            var outboxDeleted = await outboxCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (total > 0 || outboxDeleted > 0)
            {
                _logger.LogInformation(
                    "已清理用户消息与 Outbox。用户={UserId}；删除消息={Deleted}；删除Outbox={OutboxDeleted}",
                    userId,
                    total,
                    outboxDeleted);
            }
        }

        return total;
    }

    private async Task DeleteConversationDataForUserIfPresentAsync(
        NpgsqlConnection connection,
        long userId,
        CancellationToken ct)
    {
        await using (var existsCmd = new NpgsqlCommand(
                           """
                           SELECT to_regclass(@qualified) IS NOT NULL;
                           """,
                           connection))
        {
            existsCmd.Parameters.AddWithValue(
                "qualified",
                $"{_databaseSchema.Schema}.conversation_members");
            var exists = await existsCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (exists is not true)
                return;
        }

        // Direct：删除双方会话（消息已按 sender/receiver 清掉，留下空壳无意义）。
        // 非 Direct：墓碑删除成员，并清理仍指向已删用户的 tip / 未读。
        await using (var directCleanup = new NpgsqlCommand(
                           $"""
                            WITH user_direct AS (
                                SELECT m.conversation_id
                                FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                                    ON c.conversation_id = m.conversation_id
                                WHERE m.user_id = @user_id
                                  AND c.type = @direct_type
                            ),
                            delete_direct_members AS (
                                DELETE FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                USING user_direct d
                                WHERE m.conversation_id = d.conversation_id
                                RETURNING m.conversation_id
                            )
                            DELETE FROM {_databaseSchema.ConversationsTableSql} AS c
                            USING user_direct d
                            WHERE c.conversation_id = d.conversation_id;
                            """,
                           connection))
        {
            directCleanup.Parameters.AddWithValue("user_id", userId);
            directCleanup.Parameters.AddWithValue("direct_type", (short)ConversationType.Direct);
            await directCleanup.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var tombstone = new NpgsqlCommand(
                           $"""
                            WITH removed AS (
                                DELETE FROM {_databaseSchema.ConversationMembersTableSql}
                                WHERE user_id = @user_id
                                RETURNING conversation_id
                            ),
                            clear_tip AS (
                                UPDATE {_databaseSchema.ConversationsTableSql} AS c
                                SET last_message_id = NULL,
                                    last_message_preview = NULL,
                                    last_message_at_ms = NULL,
                                    last_sender_user_id = NULL,
                                    updated_at_ms = @now
                                FROM removed r
                                WHERE c.conversation_id = r.conversation_id
                                  AND c.last_sender_user_id = @user_id
                                RETURNING c.conversation_id
                            ),
                            fix_peer AS (
                                UPDATE {_databaseSchema.ConversationMembersTableSql} AS m
                                SET peer_user_id = CASE WHEN m.peer_user_id = @user_id THEN NULL ELSE m.peer_user_id END,
                                    unread_count = 0,
                                    last_read_message_id = NULL,
                                    last_read_at_ms = NULL
                                FROM removed r
                                WHERE m.conversation_id = r.conversation_id
                                  AND m.user_id <> @user_id
                                RETURNING m.user_id
                            )
                            SELECT
                                (SELECT COUNT(*) FROM removed) AS removed_members,
                                (SELECT COUNT(*) FROM clear_tip) AS cleared_tips,
                                (SELECT COUNT(*) FROM fix_peer) AS fixed_peers;
                            """,
                           connection))
        {
            tombstone.Parameters.AddWithValue("user_id", userId);
            tombstone.Parameters.AddWithValue(
                "now",
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            await tombstone.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var orphanConversationsCmd = new NpgsqlCommand(
                           $"""
                            DELETE FROM {_databaseSchema.ConversationsTableSql} AS c
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                WHERE m.conversation_id = c.conversation_id
                            );
                            """,
                           connection))
        {
            await orphanConversationsCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public async Task EnqueueEventAsync(RealtimeEvent eventToPublish, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventToPublish);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventToPublish.EventId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        await InsertOutboxAsync(connection, transaction, eventToPublish, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
    }
}

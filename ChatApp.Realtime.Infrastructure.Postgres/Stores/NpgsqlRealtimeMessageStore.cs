using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Attachments;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Conversations;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messages;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// <see cref="IRealtimeMessageStore"/> 的 Npgsql 直连实现。
/// <para>
/// 本类只负责业务编排：在每个公共方法入口打开一个 <see cref="RealtimeWriteSession"/>，
/// 然后把消息写入、附件绑定、会话投影、未读数、Outbox 发布等职责委派给独立的 Writer，
/// 最后提交事务。SQL 与事件构造分别下沉到 <see cref="MessageWriter"/> /
/// <see cref="MessageMutationWriter"/> / <see cref="ConversationProjectionWriter"/> /
/// <see cref="AttachmentBindingWriter"/> / <see cref="PostgresOutboxWriter"/> /
/// <see cref="RealtimeMessageEventFactory"/>，单事务一致性不变、不增加数据库往返。
/// </para>
/// </summary>
public sealed class NpgsqlRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly RealtimeWriteSessionFactory _sessionFactory;
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
        _sessionFactory = new RealtimeWriteSessionFactory(databaseClient, databaseSchema);
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

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var messageWriter = new MessageWriter(session);
        var attachmentWriter = new AttachmentBindingWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        var affectedRows = await messageWriter
            .InsertAsync(message, fingerprint)
            .ConfigureAwait(false);
        if (affectedRows == 0)
        {
            var existing = await messageWriter
                .GetExistingForIdempotencyAsync(message.SenderUserId, message.ClientMessageId)
                .ConfigureAwait(false);
            var existingAttachmentIds = await messageWriter
                .ListAttachmentIdsAsync(existing.MessageId)
                .ConfigureAwait(false);
            if (!RealtimeMessageFingerprint.MatchesExisting(
                    existing.Fingerprint,
                    existing.ReceiverUserId,
                    existing.Content,
                    existingAttachmentIds,
                    fingerprint))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                    message.ClientMessageId,
                    message.SenderUserId,
                    existing.MessageId);
                return RealtimeMessagePersistResult.Conflict(existing.MessageId);
            }

            // 消息与 Outbox 同事务提交：重复投递不重建 Published/Dead（及清理后的）Outbox 行。
            await session.CommitAsync().ConfigureAwait(false);
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
                var boundRecords = await attachmentWriter
                    .BindConfirmedToMessageAsync(
                        message.MessageId,
                        message.ConversationId,
                        message.SenderUserId,
                        message.AttachmentIds)
                    .ConfigureAwait(false);
                if (boundRecords.Count != expected)
                {
                    await session.RollbackAsync().ConfigureAwait(false);
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
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    ex,
                    "附件绑定拒绝。消息={MessageId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.AttachmentBindFailed(message.MessageId);
            }
        }

        var createdEvent = RealtimeMessageEventFactory.EnrichChatMessagePayload(
            RealtimeMessageEventFactory.CopyWithMessageId(eventToPublish, message.MessageId),
            boundAttachmentRefs);
        var outboxEvents = new List<RealtimeEvent>(8);

        var isGroup = !string.IsNullOrWhiteSpace(message.ConversationId)
                      && ConversationId.IsGroup(message.ConversationId);

        if (isGroup)
        {
            var memberIds = await conversationWriter
                .ListActiveMemberUserIdsAsync(message.ConversationId!)
                .ConfigureAwait(false);

            if (memberIds.Count == 0 || !memberIds.Contains(message.SenderUserId))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    "群消息写入拒绝：发送方不是成员。消息={MessageId}；会话={ConversationId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.ConversationId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.NotAllowed(message.MessageId);
            }

            outboxEvents.Add(RealtimeMessageEventFactory.CreateGroupMessageAggregatedEvent(
                createdEvent,
                message,
                memberIds));

            await AdvanceGroupConversationAndEnqueueAsync(
                    session,
                    conversationWriter,
                    message,
                    memberIds,
                    createdEvent.TraceParent,
                    createdEvent.TraceState,
                    outboxEvents)
                .ConfigureAwait(false);
        }
        else
        {
            outboxEvents.Add(createdEvent);

            // 发送方其他在线设备回声：同事务写入；Gateway 会跳过来源 SessionId。
            if (message.SenderUserId != message.ReceiverUserId)
                outboxEvents.Add(RealtimeMessageEventFactory.CreateSenderEchoEvent(createdEvent, message.SenderUserId));

            if (!string.IsNullOrWhiteSpace(message.ConversationId))
            {
                await AdvanceDirectConversationAndEnqueueAsync(
                        conversationWriter,
                        message,
                        createdEvent.TraceParent,
                        createdEvent.TraceState,
                        outboxEvents)
                    .ConfigureAwait(false);
            }
        }

        await outboxWriter.InsertManyAsync(outboxEvents).ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "实时消息已通过 Npgsql 写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);
        return RealtimeMessagePersistResult.Created(message.MessageId);
    }

    private static async Task AdvanceGroupConversationAndEnqueueAsync(
        RealtimeWriteSession session,
        ConversationProjectionWriter conversationWriter,
        RealtimeMessageRecord message,
        IReadOnlyList<long> memberIds,
        string? traceParent,
        string? traceState,
        List<RealtimeEvent> outboxEvents)
    {
        var conversationId = message.ConversationId!;
        var preview = ConversationId.CreatePreview(message.Content);

        var (advanced, unreads) = await conversationWriter
            .TryAdvanceGroupAndIncrementUnreadAsync(
                conversationId,
                message.SenderUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs)
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

    private static async Task AdvanceDirectConversationAndEnqueueAsync(
        ConversationProjectionWriter conversationWriter,
        RealtimeMessageRecord message,
        string? traceParent,
        string? traceState,
        List<RealtimeEvent> outboxEvents)
    {
        var conversationId = message.ConversationId!;
        var preview = ConversationId.CreatePreview(message.Content);

        var (advanced, unread) = await conversationWriter
            .TryAdvanceAndIncrementUnreadAsync(
                conversationId,
                message.SenderUserId,
                message.ReceiverUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs)
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
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var outboxWriter = new PostgresOutboxWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);

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
                         session.Connection,
                         session.Transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            await using var reader = await command
                .ExecuteReaderAsync(session.CancellationToken)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(session.CancellationToken).ConfigureAwait(false))
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
                         session.Connection,
                         session.Transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            command.Parameters.AddWithValue("receiver_user_id", receipt.ReceiverUserId);
            command.Parameters.AddWithValue("occurred_at_ms", receipt.OccurredAtMs);
            var affectedRows = await command
                .ExecuteNonQueryAsync(session.CancellationToken)
                .ConfigureAwait(false);
            if (affectedRows == 0)
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId,
                    senderUserId);
            }
        }

        await outboxWriter
            .InsertAsync(RealtimeMessageEventFactory.CopyForReceipt(eventToPublish, senderUserId))
            .ConfigureAwait(false);

        if (receipt.ReceiptType == MessageReceiptType.Read
            && !string.IsNullOrWhiteSpace(conversationId))
        {
            var unreadEvent = await conversationWriter
                .TryAdvanceReadStateAsync(
                    receipt.ReceiverUserId,
                    conversationId,
                    messageReceivedAtMs,
                    receipt.MessageId)
                .ConfigureAwait(false);
            if (unreadEvent is not null)
            {
                await outboxWriter.InsertAsync(unreadEvent).ConfigureAwait(false);
            }
        }

        await session.CommitAsync().ConfigureAwait(false);

        return new MessageReceiptPersistResult(
            MessageReceiptPersistStatus.Applied,
            receipt.MessageId,
            senderUserId);
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

        var payloadFingerprint = MessageMutationWriter.ComputeMutationFingerprint(
            operation: 2,
            messageId,
            content: string.Empty);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var mutationWriter = new MessageMutationWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        var prior = await mutationWriter
            .TryReadMutationRequestAsync(senderUserId, requestId)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 2
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                return new MessageRecallPersistResult(
                    MessageRecallPersistStatus.RequestConflict,
                    messageId);
            }

            await session.CommitAsync().ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageRecallPersistResult(
                    MessageRecallPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    RecalledAtMs: prior.RecalledAtMs)
                : MessageMutationWriter.MapRecallFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        var target = await mutationWriter.ReadMessageForRecallAsync(messageId).ConfigureAwait(false);
        if (target is null)
        {
            await mutationWriter.InsertMutationRequestAsync(
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
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotFound,
                messageId);
        }

        if (target.SenderUserId != senderUserId)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (target.RecalledAtMs is long already)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: already)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                already);
        }

        if (recalledAtMs - target.ReceivedAtMs > maxAgeMs)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_window_expired",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.WindowExpired,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        var affected = await mutationWriter
            .ApplyRecallUpdateAsync(messageId, recalledAtMs)
            .ConfigureAwait(false);
        if (affected == 0)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: recalledAtMs)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                recalledAtMs);
        }

        var tipPreviewUpdated = false;
        if (!string.IsNullOrWhiteSpace(target.ConversationId))
        {
            tipPreviewUpdated = await conversationWriter
                .TryUpdateConversationTipPreviewAsync(
                    target.ConversationId,
                    messageId,
                    "消息已撤回")
                .ConfigureAwait(false);
        }

        var payloadJson = JsonSerializer.Serialize(
            new RealtimeMessageRecalledPayload
            {
                MessageId = messageId,
                ConversationId = target.ConversationId,
                SenderUserId = senderUserId,
                ReceiverUserId = target.ReceiverUserId,
                RecalledAtMs = recalledAtMs
            },
            RealtimeJsonSerializerContext.Default.RealtimeMessageRecalledPayload);

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var events = new List<RealtimeEvent>(8);
        var isGroup = !string.IsNullOrWhiteSpace(target.ConversationId)
                      && ConversationId.IsGroup(target.ConversationId);

        if (isGroup)
        {
            var memberIds = await conversationWriter
                .ListActiveMemberUserIdsAsync(target.ConversationId!)
                .ConfigureAwait(false);
            foreach (var targetUserId in memberIds)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageRecalledEventId(messageId, targetUserId),
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
                        target.ConversationId!,
                        targetUserId,
                        peerUserId: null,
                        messageId,
                        "消息已撤回",
                        target.ReceivedAtMs,
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
                EventId = MessageEventIdFactory.CreateMessageRecalledEventId(messageId, target.ReceiverUserId),
                Type = RealtimeEventType.MessageRecalled,
                TargetUserId = target.ReceiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = recalledAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != target.ReceiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageRecalledEventId(messageId, senderUserId),
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

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(target.ConversationId))
            {
                var cause = $"recall:{recalledAtMs}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    senderUserId,
                    target.ReceiverUserId,
                    messageId,
                    "消息已撤回",
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    target.ReceiverUserId,
                    senderUserId,
                    messageId,
                    "消息已撤回",
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }
        }

        await outboxWriter.InsertManyAsync(events).ConfigureAwait(false);
        await mutationWriter.InsertMutationRequestAsync(
                senderUserId,
                requestId,
                operation: 2,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                target.ConversationId,
                content: null,
                editVersion: null,
                editedAtMs: null,
                recalledAtMs: recalledAtMs)
            .ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);

        return new MessageRecallPersistResult(
            MessageRecallPersistStatus.Applied,
            messageId,
            target.ReceiverUserId,
            target.ConversationId,
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

        var payloadFingerprint = MessageMutationWriter.ComputeMutationFingerprint(
            operation: 1,
            messageId,
            content);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var mutationWriter = new MessageMutationWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        var prior = await mutationWriter
            .TryReadMutationRequestAsync(senderUserId, requestId)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 1
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                return new MessageEditPersistResult(
                    MessageEditPersistStatus.RequestConflict,
                    messageId);
            }

            await session.CommitAsync().ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageEditPersistResult(
                    MessageEditPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    Content: prior.Content,
                    EditVersion: prior.EditVersion,
                    EditedAtMs: prior.EditedAtMs)
                : MessageMutationWriter.MapEditFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        var target = await mutationWriter.ReadMessageForEditAsync(messageId).ConfigureAwait(false);
        if (target is null)
        {
            await mutationWriter.InsertMutationRequestAsync(
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
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotFound,
                messageId);
        }

        if (target.SenderUserId != senderUserId)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (target.RecalledAtMs is not null)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_recalled",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: target.RecalledAtMs)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (editedAtMs - target.ReceivedAtMs > maxAgeMs)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_window_expired",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.WindowExpired,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (string.Equals(target.Content, content, StringComparison.Ordinal))
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: target.Content,
                    editVersion: target.EditVersion,
                    editedAtMs: editedAtMs,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                target.Content,
                target.EditVersion,
                editedAtMs);
        }

        var nextVersion = target.EditVersion + 1;
        var affected = await mutationWriter
            .ApplyEditUpdateAsync(messageId, content, nextVersion, editedAtMs)
            .ConfigureAwait(false);
        if (affected == 0)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_recalled",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        var tipPreviewUpdated = false;
        var tipPreview = ConversationId.CreatePreview(content);
        if (!string.IsNullOrWhiteSpace(target.ConversationId))
        {
            tipPreviewUpdated = await conversationWriter
                .TryUpdateConversationTipPreviewAsync(
                    target.ConversationId,
                    messageId,
                    tipPreview)
                .ConfigureAwait(false);
        }

        var payloadJson = JsonSerializer.Serialize(
            new RealtimeMessageEditedPayload
            {
                MessageId = messageId,
                ConversationId = target.ConversationId,
                SenderUserId = senderUserId,
                ReceiverUserId = target.ReceiverUserId,
                Content = content,
                EditVersion = nextVersion,
                EditedAtMs = editedAtMs
            },
            RealtimeJsonSerializerContext.Default.RealtimeMessageEditedPayload);

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var events = new List<RealtimeEvent>(8);
        var isGroup = !string.IsNullOrWhiteSpace(target.ConversationId)
                      && ConversationId.IsGroup(target.ConversationId);

        if (isGroup)
        {
            var memberIds = await conversationWriter
                .ListActiveMemberUserIdsAsync(target.ConversationId!)
                .ConfigureAwait(false);
            foreach (var targetUserId in memberIds)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageEditedEventId(
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
                        target.ConversationId!,
                        targetUserId,
                        peerUserId: null,
                        messageId,
                        tipPreview,
                        target.ReceivedAtMs,
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
                EventId = MessageEventIdFactory.CreateMessageEditedEventId(
                    messageId,
                    target.ReceiverUserId,
                    nextVersion),
                Type = RealtimeEventType.MessageEdited,
                TargetUserId = target.ReceiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = editedAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != target.ReceiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageEditedEventId(
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

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(target.ConversationId))
            {
                var cause = $"edit:{nextVersion}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    senderUserId,
                    target.ReceiverUserId,
                    messageId,
                    tipPreview,
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    target.ReceiverUserId,
                    senderUserId,
                    messageId,
                    tipPreview,
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }
        }

        await outboxWriter.InsertManyAsync(events).ConfigureAwait(false);
        await mutationWriter.InsertMutationRequestAsync(
                senderUserId,
                requestId,
                operation: 1,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                target.ConversationId,
                content: content,
                editVersion: nextVersion,
                editedAtMs: editedAtMs,
                recalledAtMs: null)
            .ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);

        return new MessageEditPersistResult(
            MessageEditPersistStatus.Applied,
            messageId,
            target.ReceiverUserId,
            target.ConversationId,
            content,
            nextVersion,
            editedAtMs);
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

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var outboxWriter = new PostgresOutboxWriter(session);
        await outboxWriter.InsertAsync(eventToPublish).ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);
    }
}

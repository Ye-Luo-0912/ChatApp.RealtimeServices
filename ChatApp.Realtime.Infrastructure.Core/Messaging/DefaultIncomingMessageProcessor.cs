using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultIncomingMessageProcessor : IIncomingMessageProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeMetrics _metrics;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;
    private readonly ICommandIdempotencyLedger _idempotencyLedger;
    private readonly ILogger<DefaultIncomingMessageProcessor> _logger;

    public DefaultIncomingMessageProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        IUserDeletionTombstoneStore tombstoneStore,
        ICommandIdempotencyLedger idempotencyLedger,
        ILogger<DefaultIncomingMessageProcessor> logger)
    {
        _messageStore = messageStore;
        _outboxSignal = outboxSignal;
        _metrics = metrics;
        _tombstoneStore = tombstoneStore;
        _idempotencyLedger = idempotencyLedger;
        _logger = logger;
    }

    public async Task<MessageProcessResult> ProcessAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
        {
            _metrics.RecordProcessingFailure("validation");
            return validationError;
        }

        // LongTerm-1：账号已注销用户的旧命令回放必须直接拒绝，防止 retention GC
        // 清理消息行后 JetStream replay 将旧命令当作新消息重新写入。
        if (await _tombstoneStore.IsUserDeletedAsync(command.SenderUserId, ct).ConfigureAwait(false))
        {
            _metrics.RecordProcessingFailure("user_deleted");
            _logger.LogWarning(
                "入站消息被拒绝：发送用户已注销。发送用户={SenderUserId}；命令编号={CommandId}",
                command.SenderUserId,
                command.CommandId);
            return MessageProcessResult.Failed(
                "user_deleted",
                "发送用户已注销，旧命令不再处理。",
                MessageFailureKind.Permanent);
        }

        string conversationId;
        long receiverUserId;
        var explicitConversationId = string.IsNullOrWhiteSpace(command.ConversationId)
            ? null
            : command.ConversationId.Trim();

        if (explicitConversationId is not null && ConversationId.IsGroup(explicitConversationId))
        {
            conversationId = explicitConversationId;
            receiverUserId = 0;
            // Perf-2：删除 Processor 的群成员预检查。该查询不具备事务权威性
            // （查询成功后用户仍可能立即被移除），且每条群消息多一次数据库往返。
            // 由 NpgsqlRealtimeMessageStore.SaveAsync 在写事务内加载成员并验证，
            // 失败时返回 IsNotAllowed，由下方分支统一处理。
        }
        else
        {
            if (command.ReceiverUserId <= 0)
            {
                _metrics.RecordProcessingFailure("validation");
                return MessageProcessResult.Failed(
                    "invalid_user_id",
                    "发送方和接收方用户编号必须大于 0。");
            }

            if (command.SenderUserId == command.ReceiverUserId)
            {
                _metrics.RecordProcessingFailure("validation");
                return MessageProcessResult.Failed(
                    "invalid_self_chat",
                    "单聊发送方与接收方不能为同一用户。");
            }

            conversationId = ConversationId.CreateDirect(
                command.SenderUserId,
                command.ReceiverUserId);
            receiverUserId = command.ReceiverUserId;
        }

        // LongTerm-1：独立幂等账本检查。解耦幂等性依据与 messages 行生命周期：
        // 消息行被 retention GC 或账号删除清理后，账本仍保留命令处理结果，
        // 防止 JetStream replay 将旧命令当作新消息重新写入。
        var fingerprint = RealtimeMessageFingerprint.Compute(
            receiverUserId,
            command.Content,
            command.AttachmentIds);

        var ledgerEntry = await _idempotencyLedger
            .FindAsync(command.SenderUserId, command.ClientMessageId, ct)
            .ConfigureAwait(false);
        if (ledgerEntry is not null)
        {
            if (string.Equals(ledgerEntry.ContentFingerprint, fingerprint, StringComparison.Ordinal))
            {
                // 幂等重放：内容指纹匹配，返回已有消息编号，不调用 Store。
                _metrics.RecordDuplicate();
                _outboxSignal.Notify();
                _logger.LogDebug(
                    "幂等账本命中（重放）。发送用户={SenderUserId}；客户端消息编号={ClientMessageId}；已有消息={MessageId}",
                    command.SenderUserId,
                    command.ClientMessageId,
                    ledgerEntry.MessageId);
                return MessageProcessResult.Success(ledgerEntry.MessageId ?? command.CommandId);
            }

            // 内容冲突：相同 (sender, client_message_id) 但指纹不一致。
            _metrics.RecordIdempotencyConflict();
            _metrics.RecordProcessingFailure("idempotency_conflict");
            _logger.LogWarning(
                "幂等账本命中（冲突）。发送用户={SenderUserId}；客户端消息编号={ClientMessageId}；已有消息={MessageId}",
                command.SenderUserId,
                command.ClientMessageId,
                ledgerEntry.MessageId);
            return MessageProcessResult.Failed(
                "idempotency_conflict",
                "相同客户端消息编号已存在但内容不一致。",
                MessageFailureKind.Permanent);
        }

        var record = new RealtimeMessageRecord
        {
            MessageId = command.CommandId,
            ClientMessageId = command.ClientMessageId,
            SenderUserId = command.SenderUserId,
            SenderSessionId = command.SenderSessionId,
            ReceiverUserId = receiverUserId,
            ConversationId = conversationId,
            Content = command.Content,
            AttachmentIds = command.AttachmentIds,
            ReplyToMessageId = command.ReplyToMessageId,
            ReplyToSenderUserId = command.ReplyToSenderUserId,
            ReplyToPreview = command.ReplyToPreview,
            ForwardedFromMessageId = command.ForwardedFromMessageId,
            ForwardedFromSenderUserId = command.ForwardedFromSenderUserId,
            ForwardedFromPreview = command.ForwardedFromPreview,
            MentionedUserIds = command.MentionedUserIds,
            MentionedRoles = command.MentionedRoles,
            ReceivedAtMs = command.ReceivedAtMs
        };

        var evt = new RealtimeEvent
        {
            EventId = ConversationId.IsGroup(conversationId)
                ? MessageEventIdFactory.CreateMessageReceivedEventId(
                    command.SenderUserId,
                    command.ClientMessageId,
                    command.SenderUserId)
                : MessageEventIdFactory.CreateMessageReceivedEventId(
                    command.SenderUserId,
                    command.ClientMessageId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = ConversationId.IsGroup(conversationId)
                ? command.SenderUserId
                : command.ReceiverUserId,
            ActorUserId = command.SenderUserId,
            MessageId = command.CommandId,
            SessionId = command.SenderSessionId,
            // P1-4：直接传 payload 对象给 Store，由 Store 在附件绑定后调用
            // EnrichChatMessagePayload 一次性物化为 PayloadJson，省去 Processor
            // 序列化 + Store 反序列化的重复工作。Outbox 仅看到物化后的 PayloadJson。
            Payload = new RealtimeChatMessagePayload
            {
                MessageId = command.CommandId,
                ClientMessageId = command.ClientMessageId,
                SenderUserId = command.SenderUserId,
                SenderSessionId = command.SenderSessionId,
                ReceiverUserId = receiverUserId,
                ConversationId = conversationId,
                Content = command.Content,
                ReceivedAtMs = command.ReceivedAtMs,
                ReplyToMessageId = command.ReplyToMessageId,
                ReplyToSenderUserId = command.ReplyToSenderUserId,
                ReplyToPreview = command.ReplyToPreview,
                ForwardedFromMessageId = command.ForwardedFromMessageId,
                ForwardedFromSenderUserId = command.ForwardedFromSenderUserId,
                ForwardedFromPreview = command.ForwardedFromPreview,
                MentionedUserIds = command.MentionedUserIds,
                MentionedRoles = command.MentionedRoles
            },
            OccurredAtMs = command.ReceivedAtMs,
            TraceParent = RealtimeTraceContext.CaptureTraceParent(),
            TraceState = RealtimeTraceContext.CaptureTraceState()
        };

        var persisted = await _messageStore.SaveAsync(record, evt, ct).ConfigureAwait(false);
        if (persisted.IsConflict)
        {
            _metrics.RecordIdempotencyConflict();
            _metrics.RecordProcessingFailure("idempotency_conflict");
            _logger.LogWarning(
                "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                record.ClientMessageId,
                record.SenderUserId,
                persisted.MessageId);
            // LongTerm-1：回填账本（messages 表冲突但账本未命中，说明是迁移前数据）。
            await RecordLedgerBestEffortAsync(
                command.CommandId,
                command.SenderUserId,
                command.ClientMessageId,
                fingerprint,
                IdempotencyLedgerResultKind.Conflict,
                persisted.MessageId,
                command.ReceivedAtMs,
                ct).ConfigureAwait(false);
            return MessageProcessResult.Failed(
                "idempotency_conflict",
                "相同客户端消息编号已存在但内容不一致。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsAttachmentBindFailed)
        {
            _metrics.RecordProcessingFailure("attachment_bind_failed");
            _logger.LogWarning(
                "入站消息附件绑定失败。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；消息={MessageId}",
                record.ClientMessageId,
                record.SenderUserId,
                persisted.MessageId);
            // 附件绑定失败不记录账本：重试可能成功（附件状态可能变化）。
            return MessageProcessResult.Failed(
                "attachment_bind_failed",
                "附件不存在、未确认或不属于发送方，消息未写入。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsNotAllowed)
        {
            _metrics.RecordProcessingFailure("not_member");
            // 权限失败不记录账本：重试可能成功（成员关系可能变化）。
            return MessageProcessResult.Failed(
                "forbidden",
                "无权在该会话发送消息。",
                MessageFailureKind.Permanent);
        }

        _outboxSignal.Notify();

        if (!persisted.IsNew)
        {
            _logger.LogDebug(
                "重复入站消息已完成幂等处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                persisted.MessageId,
                record.SenderUserId,
                record.ReceiverUserId);

            _metrics.RecordDuplicate();
            // LongTerm-1：回填账本（messages 表命中但账本未命中，说明是迁移前数据）。
            await RecordLedgerBestEffortAsync(
                command.CommandId,
                command.SenderUserId,
                command.ClientMessageId,
                fingerprint,
                IdempotencyLedgerResultKind.Duplicate,
                persisted.MessageId,
                command.ReceivedAtMs,
                ct).ConfigureAwait(false);
            return MessageProcessResult.Success(persisted.MessageId);
        }

        _metrics.RecordPersisted();
        // LongTerm-1：记录 Created 结果到独立账本，解耦幂等性与 messages 行生命周期。
        await RecordLedgerBestEffortAsync(
            command.CommandId,
            command.SenderUserId,
            command.ClientMessageId,
            fingerprint,
            IdempotencyLedgerResultKind.Created,
            persisted.MessageId,
            command.ReceivedAtMs,
            ct).ConfigureAwait(false);

        _logger.LogDebug(
            "入站消息已处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            record.MessageId,
            record.SenderUserId,
            record.ReceiverUserId);

        return MessageProcessResult.Success(record.MessageId);
    }

    /// <summary>
    /// LongTerm-1：best-effort 记录幂等账本。写入失败不阻断主流程（消息已持久化）。
    /// 失败时仅记录日志：retention GC 后旧命令可能"复活"，但 tombstone 检查会拒绝已注销用户，
    /// 且 retention GC 周期远大于此 crash 窗口。
    /// </summary>
    private async Task RecordLedgerBestEffortAsync(
        string commandId,
        long senderUserId,
        string clientMessageId,
        string fingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct)
    {
        try
        {
            await _idempotencyLedger
                .RecordAsync(commandId, senderUserId, clientMessageId,
                    fingerprint, kind, messageId, receivedAtMs, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "幂等账本写入失败（不阻断主流程）。发送用户={SenderUserId}；客户端消息编号={ClientMessageId}",
                senderUserId,
                clientMessageId);
        }
    }

    private static MessageProcessResult? Validate(IncomingMessageCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.CommandId) || command.CommandId.Length > 64)
            return MessageProcessResult.Failed("invalid_command_id", "命令编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.ClientMessageId) || command.ClientMessageId.Length > 128)
            return MessageProcessResult.Failed("invalid_client_message_id", "客户端消息编号不能为空且长度不能超过 128。");
        if (command.SenderUserId <= 0)
            return MessageProcessResult.Failed("invalid_user_id", "发送方用户编号必须大于 0。");

        var conversationId = string.IsNullOrWhiteSpace(command.ConversationId)
            ? null
            : command.ConversationId.Trim();
        if (conversationId is not null)
        {
            if (conversationId.Length > ConversationId.MaxLength
                || (!ConversationId.IsGroup(conversationId) && !ConversationId.IsDirect(conversationId)))
            {
                return MessageProcessResult.Failed("invalid_conversation_id", "会话编号无效。");
            }
        }

        if (string.IsNullOrWhiteSpace(command.SenderSessionId) || command.SenderSessionId.Length > 128)
            return MessageProcessResult.Failed("invalid_session_id", "发送会话编号不能为空且长度不能超过 128。");
        if (string.IsNullOrWhiteSpace(command.Content)
            && command.AttachmentIds is not { Count: > 0 })
            return MessageProcessResult.Failed("empty_content", "入站消息内容不能为空。");
        if (command.Content.Length > 65_536)
            return MessageProcessResult.Failed("content_too_large", "入站消息内容不能超过 65536 个字符。");
        if (command.AttachmentIds is { Count: > 0 })
        {
            if (command.AttachmentIds.Count > 32)
                return MessageProcessResult.Failed(
                    "too_many_attachments",
                    "单条消息附件数不能超过 32。");
            foreach (var id in command.AttachmentIds)
            {
                if (string.IsNullOrWhiteSpace(id) || id.Length > 64)
                    return MessageProcessResult.Failed(
                        "invalid_attachment_id",
                        "附件编号不能为空且长度不能超过 64。");
            }
        }

        if (!string.IsNullOrWhiteSpace(command.ReplyToMessageId))
        {
            if (command.ReplyToMessageId.Length > 64)
                return MessageProcessResult.Failed(
                    "invalid_reply_to_message_id",
                    "回复目标消息编号长度不能超过 64。");
            if (command.ReplyToSenderUserId is null or <= 0)
                return MessageProcessResult.Failed(
                    "invalid_reply_to_sender",
                    "回复目标发送方用户编号必须大于 0。");
            if (command.ReplyToPreview is { Length: > 256 })
                return MessageProcessResult.Failed(
                    "invalid_reply_to_preview",
                    "回复预览长度不能超过 256。");
        }
        else if (command.ReplyToSenderUserId is not null
                 || !string.IsNullOrWhiteSpace(command.ReplyToPreview))
        {
            return MessageProcessResult.Failed(
                "invalid_reply_to",
                "缺少回复目标消息编号时不能携带回复元数据。");
        }

        if (!string.IsNullOrWhiteSpace(command.ForwardedFromMessageId)
            && !string.IsNullOrWhiteSpace(command.ReplyToMessageId))
        {
            return MessageProcessResult.Failed(
                "invalid_reply_and_forward",
                "同一条消息不能同时回复与转发。");
        }

        if (!string.IsNullOrWhiteSpace(command.ForwardedFromMessageId))
        {
            if (command.ForwardedFromMessageId.Length > 64)
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_message_id",
                    "转发原消息编号长度不能超过 64。");
            if (command.ForwardedFromSenderUserId is null or <= 0)
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_sender",
                    "转发原发送方用户编号必须大于 0。");
            if (command.ForwardedFromPreview is { Length: > 256 })
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_preview",
                    "转发预览长度不能超过 256。");
        }
        else if (command.ForwardedFromSenderUserId is not null
                 || !string.IsNullOrWhiteSpace(command.ForwardedFromPreview))
        {
            return MessageProcessResult.Failed(
                "invalid_forwarded_from",
                "缺少转发原消息编号时不能携带转发元数据。");
        }

        return null;
    }
}

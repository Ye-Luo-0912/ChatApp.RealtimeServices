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
    private readonly IRealtimeGroupStore _groupStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultIncomingMessageProcessor> _logger;

    public DefaultIncomingMessageProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeGroupStore groupStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultIncomingMessageProcessor> logger)
    {
        _messageStore = messageStore;
        _groupStore = groupStore;
        _outboxSignal = outboxSignal;
        _metrics = metrics;
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

        string conversationId;
        long receiverUserId;
        var explicitConversationId = string.IsNullOrWhiteSpace(command.ConversationId)
            ? null
            : command.ConversationId.Trim();

        if (explicitConversationId is not null && ConversationId.IsGroup(explicitConversationId))
        {
            conversationId = explicitConversationId;
            receiverUserId = 0;
            var isMember = await _groupStore
                .IsActiveMemberAsync(conversationId, command.SenderUserId, ct)
                .ConfigureAwait(false);
            if (!isMember)
            {
                _metrics.RecordProcessingFailure("not_member");
                return MessageProcessResult.Failed(
                    "forbidden",
                    "无权在该群发送消息。",
                    MessageFailureKind.Permanent);
            }
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
            return MessageProcessResult.Failed(
                "attachment_bind_failed",
                "附件不存在、未确认或不属于发送方，消息未写入。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsNotAllowed)
        {
            _metrics.RecordProcessingFailure("not_member");
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
            return MessageProcessResult.Success(persisted.MessageId);
        }

        _metrics.RecordPersisted();

        _logger.LogDebug(
            "入站消息已处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            record.MessageId,
            record.SenderUserId,
            record.ReceiverUserId);

        return MessageProcessResult.Success(record.MessageId);
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

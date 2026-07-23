using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultMessageReceiptProcessor : IMessageReceiptProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultMessageReceiptProcessor> _logger;

    public DefaultMessageReceiptProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultMessageReceiptProcessor> logger)
    {
        _messageStore = messageStore;
        _outboxSignal = outboxSignal;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<MessageProcessResult> ProcessAsync(
        MessageReceiptCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
        {
            _metrics.RecordProcessingFailure("receipt_validation");
            return validationError;
        }

        var receipt = new MessageReceiptRecord
        {
            CommandId = command.CommandId,
            MessageId = command.MessageId,
            ReceiverUserId = command.ReceiverUserId,
            ReceiverSessionId = command.ReceiverSessionId,
            ReceiptType = command.ReceiptType,
            OccurredAtMs = command.OccurredAtMs
        };

        var evt = new RealtimeEvent
        {
            EventId = CreateEventId(command),
            Type = RealtimeEventType.MessageReceiptUpdated,
            TargetUserId = 0,
            ActorUserId = command.ReceiverUserId,
            MessageId = command.MessageId,
            SessionId = command.ReceiverSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeMessageReceiptPayload
                {
                    MessageId = command.MessageId,
                    ReceiverUserId = command.ReceiverUserId,
                    ReceiptType = command.ReceiptType,
                    OccurredAtMs = command.OccurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeMessageReceiptPayload),
            OccurredAtMs = command.OccurredAtMs,
            TraceParent = RealtimeTraceContext.CaptureTraceParent(),
            TraceState = RealtimeTraceContext.CaptureTraceState()
        };

        var result = await _messageStore
            .ApplyReceiptAsync(receipt, evt, ct)
            .ConfigureAwait(false);

        switch (result.Status)
        {
            case MessageReceiptPersistStatus.Applied:
                _outboxSignal.Notify();
                _metrics.RecordReceiptApplied(command.ReceiptType);
                _logger.LogInformation(
                    "消息回执已持久化。消息编号={MessageId}；接收用户={ReceiverUserId}；类型={ReceiptType}",
                    command.MessageId,
                    command.ReceiverUserId,
                    command.ReceiptType);
                return MessageProcessResult.Success(command.MessageId);

            case MessageReceiptPersistStatus.Unchanged:
                _metrics.RecordReceiptDuplicate(command.ReceiptType);
                return MessageProcessResult.Success(command.MessageId);

            case MessageReceiptPersistStatus.MessageNotFound:
                return MessageProcessResult.Failed(
                    "message_not_found",
                    "回执对应的消息不存在。");

            case MessageReceiptPersistStatus.ReceiverMismatch:
                return MessageProcessResult.Failed(
                    "receipt_not_allowed",
                    "当前用户不是该消息的接收者。");

            default:
                throw new InvalidOperationException("未知的消息回执持久化结果。");
        }
    }

    private static MessageProcessResult? Validate(MessageReceiptCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.CommandId) || command.CommandId.Length > 64)
            return MessageProcessResult.Failed("invalid_command_id", "命令编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.MessageId) || command.MessageId.Length > 64)
            return MessageProcessResult.Failed("invalid_message_id", "消息编号不能为空且长度不能超过 64。");
        if (command.ReceiverUserId <= 0)
            return MessageProcessResult.Failed("invalid_receiver_user_id", "接收用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.ReceiverSessionId) || command.ReceiverSessionId.Length > 128)
            return MessageProcessResult.Failed("invalid_session_id", "接收会话编号不能为空且长度不能超过 128。");
        if (!Enum.IsDefined(command.ReceiptType))
            return MessageProcessResult.Failed("invalid_receipt_type", "消息回执类型无效。");
        if (command.OccurredAtMs <= 0)
            return MessageProcessResult.Failed("invalid_occurred_at", "消息回执时间必须大于 0。");

        return null;
    }

    private static string CreateEventId(MessageReceiptCommand command) =>
        RealtimeEventContracts.CreateMessageReceiptUpdatedEventId(
            command.MessageId,
            command.ReceiverUserId,
            command.ReceiptType);
}
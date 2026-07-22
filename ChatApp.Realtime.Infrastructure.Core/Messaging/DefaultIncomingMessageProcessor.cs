using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultIncomingMessageProcessor : IIncomingMessageProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultIncomingMessageProcessor> _logger;

    public DefaultIncomingMessageProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultIncomingMessageProcessor> logger)
    {
        _messageStore = messageStore;
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

        var record = new RealtimeMessageRecord
        {
            MessageId = command.CommandId,
            ClientMessageId = command.ClientMessageId,
            SenderUserId = command.SenderUserId,
            SenderSessionId = command.SenderSessionId,
            ReceiverUserId = command.ReceiverUserId,
            Content = command.Content,
            ReceivedAtMs = command.ReceivedAtMs
        };

        var evt = new RealtimeEvent
        {
            EventId = CreateEventId(command),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = command.ReceiverUserId,
            ActorUserId = command.SenderUserId,
            MessageId = command.CommandId,
            SessionId = command.SenderSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = command.CommandId,
                    ClientMessageId = command.ClientMessageId,
                    SenderUserId = command.SenderUserId,
                    SenderSessionId = command.SenderSessionId,
                    ReceiverUserId = command.ReceiverUserId,
                    Content = command.Content,
                    ReceivedAtMs = command.ReceivedAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = command.ReceivedAtMs,
            TraceParent = RealtimeTraceContext.CaptureTraceParent(),
            TraceState = RealtimeTraceContext.CaptureTraceState()
        };

        var persisted = await _messageStore.SaveAsync(record, evt, ct).ConfigureAwait(false);
        _outboxSignal.Notify();

        if (!persisted.IsNew)
        {
            _logger.LogInformation(
                "重复入站消息已完成幂等处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                persisted.MessageId,
                record.SenderUserId,
                record.ReceiverUserId);

            _metrics.RecordDuplicate();
            return MessageProcessResult.Success(persisted.MessageId);
        }

        _metrics.RecordPersisted();

        _logger.LogInformation(
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
        if (command.SenderUserId <= 0 || command.ReceiverUserId <= 0)
            return MessageProcessResult.Failed("invalid_user_id", "发送方和接收方用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.SenderSessionId) || command.SenderSessionId.Length > 128)
            return MessageProcessResult.Failed("invalid_session_id", "发送会话编号不能为空且长度不能超过 128。");
        if (string.IsNullOrWhiteSpace(command.Content))
            return MessageProcessResult.Failed("empty_content", "入站消息内容不能为空。");
        if (command.Content.Length > 65_536)
            return MessageProcessResult.Failed("content_too_large", "入站消息内容不能超过 65536 个字符。");

        return null;
    }

    private static string CreateEventId(IncomingMessageCommand command)
    {
        var input = Encoding.UTF8.GetBytes($"{command.SenderUserId}:{command.ClientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }
}

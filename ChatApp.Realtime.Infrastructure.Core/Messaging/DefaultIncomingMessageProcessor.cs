using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultIncomingMessageProcessor : IIncomingMessageProcessor
{
    
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeEventPublisher _eventPublisher;
    private readonly ILogger<DefaultIncomingMessageProcessor> _logger;

    public DefaultIncomingMessageProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeEventPublisher eventPublisher,
        ILogger<DefaultIncomingMessageProcessor> logger)
    {
        _messageStore = messageStore;
        _eventPublisher = eventPublisher;
        _logger = logger;
    }

    public async Task<MessageProcessResult> ProcessAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(command.Content))
        {
            return MessageProcessResult.Failed("empty_content", "入站消息内容不能为空。");
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

        var isNew = await _messageStore.SaveAsync(record, ct).ConfigureAwait(false);

        if (!isNew)
        {
            _logger.LogInformation(
                "重复入站消息已跳过事件发布。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                record.MessageId,
                record.SenderUserId,
                record.ReceiverUserId);

            return MessageProcessResult.Success(record.MessageId);
        }

        await _eventPublisher.PublishAsync(
            new RealtimeEvent
            {
                EventId = Guid.NewGuid().ToString("N"),
                Type = RealtimeEventType.MessageReceived,
                TargetUserId = command.ReceiverUserId,
                ActorUserId = command.SenderUserId,
                SessionId = command.SenderSessionId,
                OccurredAtMs = command.ReceivedAtMs
            },
            ct).ConfigureAwait(false);

        _logger.LogInformation(
            "入站消息已处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            record.MessageId,
            record.SenderUserId,
            record.ReceiverUserId);

        return MessageProcessResult.Success(record.MessageId);
    }
}

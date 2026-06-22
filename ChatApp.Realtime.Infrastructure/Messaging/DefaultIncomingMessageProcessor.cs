using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Messaging;

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

    /// <summary>
    /// 异步处理入站消息。
    /// </summary>
    /// <param name="command">包含入站消息信息的命令对象。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <returns>返回一个表示消息处理结果的对象。如果消息内容为空，则返回失败的结果；否则，保存消息记录并发布实时事件后，返回成功的结果。</returns>
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

        await _messageStore.SaveAsync(record, ct).ConfigureAwait(false);

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

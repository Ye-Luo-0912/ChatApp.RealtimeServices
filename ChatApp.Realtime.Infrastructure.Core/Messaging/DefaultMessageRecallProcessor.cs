using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultMessageRecallProcessor : IMessageRecallProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly MessageRecallOptions _options;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultMessageRecallProcessor> _logger;

    public DefaultMessageRecallProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultMessageRecallProcessor> logger,
        MessageRecallOptions? options = null)
    {
        _messageStore = messageStore;
        _outboxSignal = outboxSignal;
        _options = options ?? new MessageRecallOptions();
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<MessageRecallResult> ProcessAsync(
        MessageRecallCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        var maxAgeMs = _options.MaxAgeMs;
        if (maxAgeMs <= 0)
        {
            return MessageRecallResult.Failed(
                command.RequestId,
                "recall_disabled",
                "消息撤回已关闭。");
        }

        var result = await _messageStore
            .ApplyRecallAsync(
                command.RequestId.Trim(),
                command.MessageId.Trim(),
                command.SenderUserId,
                command.SenderSessionId,
                command.OccurredAtMs,
                maxAgeMs,
                ct)
            .ConfigureAwait(false);

        switch (result.Status)
        {
            case MessageRecallPersistStatus.Applied:
                _outboxSignal.Notify();
                _metrics.RecordPersisted();
                _logger.LogInformation(
                    "消息已撤回。消息编号={MessageId}；发送用户={SenderUserId}",
                    command.MessageId,
                    command.SenderUserId);
                return MessageRecallResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.RecalledAtMs ?? command.OccurredAtMs);

            case MessageRecallPersistStatus.Unchanged:
                return MessageRecallResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.RecalledAtMs ?? command.OccurredAtMs);

            case MessageRecallPersistStatus.NotFound:
                return MessageRecallResult.Failed(
                    command.RequestId,
                    "message_not_found",
                    "消息不存在。");

            case MessageRecallPersistStatus.NotAllowed:
                return MessageRecallResult.Failed(
                    command.RequestId,
                    "recall_not_allowed",
                    "仅发送方可撤回该消息。");

            case MessageRecallPersistStatus.WindowExpired:
                return MessageRecallResult.Failed(
                    command.RequestId,
                    "recall_window_expired",
                    "已超过可撤回时间。");

            case MessageRecallPersistStatus.RequestConflict:
                return MessageRecallResult.Failed(
                    command.RequestId,
                    "request_id_conflict",
                    "请求编号已用于其他撤回操作。");

            default:
                throw new InvalidOperationException("未知的消息撤回持久化结果。");
        }
    }

    private static MessageRecallResult? Validate(MessageRecallCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return MessageRecallResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.MessageId) || command.MessageId.Length > 64)
            return MessageRecallResult.Failed(
                command.RequestId,
                "invalid_message_id",
                "消息编号不能为空且长度不能超过 64。");
        if (command.SenderUserId <= 0)
            return MessageRecallResult.Failed(
                command.RequestId,
                "invalid_sender_user_id",
                "发送用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.SenderSessionId) || command.SenderSessionId.Length > 128)
            return MessageRecallResult.Failed(
                command.RequestId,
                "invalid_session_id",
                "发送会话编号不能为空且长度不能超过 128。");
        if (command.OccurredAtMs <= 0)
            return MessageRecallResult.Failed(
                command.RequestId,
                "invalid_occurred_at",
                "撤回时间必须大于 0。");

        return null;
    }
}

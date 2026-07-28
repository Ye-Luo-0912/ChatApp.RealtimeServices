using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultMessageEditProcessor : IMessageEditProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly MessageEditOptions _options;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultMessageEditProcessor> _logger;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;

    public DefaultMessageEditProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultMessageEditProcessor> logger,
        IUserDeletionTombstoneStore tombstoneStore,
        MessageEditOptions? options = null)
    {
        _messageStore = messageStore;
        _outboxSignal = outboxSignal;
        _options = options ?? new MessageEditOptions();
        _metrics = metrics;
        _logger = logger;
        _tombstoneStore = tombstoneStore;
    }

    public async Task<MessageEditResult> ProcessAsync(
        MessageEditCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        var maxAgeMs = _options.MaxAgeMs;
        if (maxAgeMs <= 0)
        {
            return MessageEditResult.Failed(
                command.RequestId,
                "edit_disabled",
                "消息编辑已关闭。");
        }

        if (await _tombstoneStore.IsUserDeletedAsync(command.SenderUserId, ct).ConfigureAwait(false))
        {
            return MessageEditResult.Failed(command.RequestId, "user_deleted", "用户已注销，操作被拒绝。");
        }

        var result = await _messageStore
            .ApplyEditAsync(
                command.RequestId.Trim(),
                command.MessageId.Trim(),
                command.SenderUserId,
                command.SenderSessionId,
                command.Content,
                command.OccurredAtMs,
                maxAgeMs,
                command.MentionedUserIds,
                command.MentionedRoles,
                ct)
            .ConfigureAwait(false);

        switch (result.Status)
        {
            case MessageEditPersistStatus.Applied:
                _outboxSignal.Notify();
                _metrics.RecordPersisted();
                _logger.LogInformation(
                    "消息已编辑。消息编号={MessageId}；版本={EditVersion}；发送用户={SenderUserId}",
                    command.MessageId,
                    result.EditVersion,
                    command.SenderUserId);
                return MessageEditResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.Content ?? command.Content,
                    result.EditVersion ?? 1,
                    result.EditedAtMs ?? command.OccurredAtMs);

            case MessageEditPersistStatus.Unchanged:
                return MessageEditResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.Content ?? command.Content,
                    result.EditVersion ?? 1,
                    result.EditedAtMs ?? command.OccurredAtMs);

            case MessageEditPersistStatus.NotFound:
                return MessageEditResult.Failed(
                    command.RequestId,
                    "message_not_found",
                    "消息不存在。");

            case MessageEditPersistStatus.NotAllowed:
                return MessageEditResult.Failed(
                    command.RequestId,
                    "edit_not_allowed",
                    "仅发送方可编辑该消息。");

            case MessageEditPersistStatus.WindowExpired:
                return MessageEditResult.Failed(
                    command.RequestId,
                    "edit_window_expired",
                    "已超过可编辑时间。");

            case MessageEditPersistStatus.AlreadyRecalled:
                return MessageEditResult.Failed(
                    command.RequestId,
                    "message_recalled",
                    "消息已撤回，无法编辑。");

            case MessageEditPersistStatus.RequestConflict:
                return MessageEditResult.Failed(
                    command.RequestId,
                    "request_id_conflict",
                    "请求编号已用于其他编辑内容。");

            default:
                throw new InvalidOperationException("未知的消息编辑持久化结果。");
        }
    }

    private static MessageEditResult? Validate(MessageEditCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return MessageEditResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.MessageId) || command.MessageId.Length > 64)
            return MessageEditResult.Failed(
                command.RequestId,
                "invalid_message_id",
                "消息编号不能为空且长度不能超过 64。");
        if (command.Content is null || command.Content.Length > 65_536)
            return MessageEditResult.Failed(
                command.RequestId,
                "invalid_content",
                "消息内容无效或过长。");
        if (command.SenderUserId <= 0)
            return MessageEditResult.Failed(
                command.RequestId,
                "invalid_sender_user_id",
                "发送用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.SenderSessionId) || command.SenderSessionId.Length > 128)
            return MessageEditResult.Failed(
                command.RequestId,
                "invalid_session_id",
                "发送会话编号不能为空且长度不能超过 128。");
        if (command.OccurredAtMs <= 0)
            return MessageEditResult.Failed(
                command.RequestId,
                "invalid_occurred_at",
                "编辑时间必须大于 0。");

        return null;
    }
}

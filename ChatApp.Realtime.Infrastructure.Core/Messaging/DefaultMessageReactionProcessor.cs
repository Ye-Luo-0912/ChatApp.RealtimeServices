using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultMessageReactionProcessor : IMessageReactionProcessor
{
    private readonly IRealtimeReactionStore _reactionStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly MessageReactionOptions _options;
    private readonly RealtimeMetrics _metrics;
    private readonly ILogger<DefaultMessageReactionProcessor> _logger;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;

    public DefaultMessageReactionProcessor(
        IRealtimeReactionStore reactionStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        ILogger<DefaultMessageReactionProcessor> logger,
        IUserDeletionTombstoneStore tombstoneStore,
        MessageReactionOptions? options = null)
    {
        _reactionStore = reactionStore;
        _outboxSignal = outboxSignal;
        _options = options ?? new MessageReactionOptions();
        _metrics = metrics;
        _logger = logger;
        _tombstoneStore = tombstoneStore;
    }

    public async Task<MessageReactionResult> ProcessAsync(
        MessageReactionCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        if (await _tombstoneStore.IsUserDeletedAsync(command.ActorUserId, ct).ConfigureAwait(false))
        {
            return MessageReactionResult.Failed(command.RequestId, "user_deleted", "用户已注销，操作被拒绝。");
        }

        var emoji = command.Emoji.Trim();
        var messageId = command.MessageId.Trim();
        MessageReactionPersistResult result;
        if (command.Action == MessageReactionAction.Add)
        {
            result = await _reactionStore
                .AddAsync(
                    messageId,
                    command.ActorUserId,
                    command.ActorSessionId,
                    emoji,
                    command.OccurredAtMs,
                    _options,
                    ct)
                .ConfigureAwait(false);
        }
        else if (command.Action == MessageReactionAction.Remove)
        {
            result = await _reactionStore
                .RemoveAsync(
                    messageId,
                    command.ActorUserId,
                    command.ActorSessionId,
                    emoji,
                    command.OccurredAtMs,
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_action",
                "反应操作无效。");
        }

        switch (result.Status)
        {
            case MessageReactionPersistStatus.Applied:
                _outboxSignal.Notify();
                _metrics.RecordPersisted();
                _logger.LogInformation(
                    "消息反应已{Action}。消息编号={MessageId}；表情={Emoji}；用户={ActorUserId}",
                    command.Action,
                    messageId,
                    emoji,
                    command.ActorUserId);
                return MessageReactionResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.Emoji ?? emoji,
                    command.Action,
                    result.OccurredAtMs ?? command.OccurredAtMs,
                    result.EmojiCount ?? 0);

            case MessageReactionPersistStatus.Unchanged:
                return MessageReactionResult.Success(
                    command.RequestId,
                    result.MessageId,
                    result.ConversationId,
                    result.Emoji ?? emoji,
                    command.Action,
                    result.OccurredAtMs ?? command.OccurredAtMs,
                    result.EmojiCount ?? 0);

            case MessageReactionPersistStatus.NotFound:
                return MessageReactionResult.Failed(
                    command.RequestId,
                    "message_not_found",
                    "消息不存在。");

            case MessageReactionPersistStatus.NotAllowed:
                return MessageReactionResult.Failed(
                    command.RequestId,
                    "reaction_not_allowed",
                    "仅会话成员可对该消息添加或移除反应。");

            case MessageReactionPersistStatus.AlreadyRecalled:
                return MessageReactionResult.Failed(
                    command.RequestId,
                    "message_recalled",
                    "消息已撤回，无法反应。");

            case MessageReactionPersistStatus.LimitExceeded:
                return MessageReactionResult.Failed(
                    command.RequestId,
                    "reaction_limit_exceeded",
                    "已达到反应数量上限。");

            default:
                throw new InvalidOperationException("未知的消息反应持久化结果。");
        }
    }

    private MessageReactionResult? Validate(MessageReactionCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return MessageReactionResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.MessageId) || command.MessageId.Length > 64)
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_message_id",
                "消息编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.Emoji)
            || command.Emoji.Trim().Length == 0
            || command.Emoji.Trim().Length > _options.MaxEmojiLength)
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_emoji",
                $"表情不能为空且长度不能超过 {_options.MaxEmojiLength}。");
        if (command.Action is not (MessageReactionAction.Add or MessageReactionAction.Remove))
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_action",
                "反应操作无效。");
        if (command.ActorUserId <= 0)
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_actor_user_id",
                "操作用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.ActorSessionId) || command.ActorSessionId.Length > 128)
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_session_id",
                "会话编号不能为空且长度不能超过 128。");
        if (command.OccurredAtMs <= 0)
            return MessageReactionResult.Failed(
                command.RequestId,
                "invalid_occurred_at",
                "反应时间必须大于 0。");
        if (_options.MaxDistinctEmojisPerMessage <= 0 || _options.MaxReactionsPerUserPerMessage <= 0)
            return MessageReactionResult.Failed(
                command.RequestId,
                "reaction_disabled",
                "消息反应已关闭。");

        return null;
    }
}

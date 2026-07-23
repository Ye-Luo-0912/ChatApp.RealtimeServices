using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class DefaultConversationSetPrefsProcessor : IConversationSetPrefsProcessor
{
    private readonly IRealtimeConversationStore _store;
    private readonly IRealtimeOutboxSignal _outboxSignal;

    public DefaultConversationSetPrefsProcessor(
        IRealtimeConversationStore store,
        IRealtimeOutboxSignal outboxSignal)
    {
        _store = store;
        _outboxSignal = outboxSignal;
    }

    public async Task<ConversationSetPrefsResult> ProcessAsync(
        ConversationSetPrefsCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        var conversationId = command.ConversationId.Trim();
        var result = await _store.SetMemberPrefsAsync(
                command.UserId,
                conversationId,
                command.Pinned,
                command.Muted,
                command.MutedUntilMs,
                ct)
            .ConfigureAwait(false);

        if (!result.Found)
        {
            return ConversationSetPrefsResult.Failed(
                command.RequestId,
                "not_found",
                "会话不存在或当前用户不是成员。");
        }

        if (result.Changed)
            _outboxSignal.Notify();

        return ConversationSetPrefsResult.Success(
            command.RequestId,
            conversationId,
            result.IsPinned,
            result.IsMuted,
            result.MutedUntilMs,
            result.Changed);
    }

    private static ConversationSetPrefsResult? Validate(ConversationSetPrefsCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return ConversationSetPrefsResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (command.UserId <= 0)
            return ConversationSetPrefsResult.Failed(
                command.RequestId,
                "invalid_user_id",
                "用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.ConversationId)
            || command.ConversationId.Length > ConversationId.MaxLength)
        {
            return ConversationSetPrefsResult.Failed(
                command.RequestId,
                "invalid_conversation_id",
                "会话编号无效。");
        }

        if (command.Pinned is null && command.Muted is null)
        {
            return ConversationSetPrefsResult.Failed(
                command.RequestId,
                "invalid_prefs",
                "至少需要指定置顶或免打扰偏好之一。");
        }

        if (command.Muted is true && command.MutedUntilMs is <= 0)
        {
            return ConversationSetPrefsResult.Failed(
                command.RequestId,
                "invalid_muted_until",
                "免打扰截止时间必须为正数 Unix 毫秒，或省略表示永久。");
        }

        return null;
    }
}

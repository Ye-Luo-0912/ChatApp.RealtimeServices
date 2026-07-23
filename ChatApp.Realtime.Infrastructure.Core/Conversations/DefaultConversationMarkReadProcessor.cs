using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class DefaultConversationMarkReadProcessor : IConversationMarkReadProcessor
{
    private readonly IRealtimeConversationStore _store;
    private readonly IRealtimeOutboxSignal _outboxSignal;

    public DefaultConversationMarkReadProcessor(
        IRealtimeConversationStore store,
        IRealtimeOutboxSignal outboxSignal)
    {
        _store = store;
        _outboxSignal = outboxSignal;
    }

    public async Task<ConversationMarkReadResult> ProcessAsync(
        ConversationMarkReadCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        var conversationId = command.ConversationId.Trim();
        var hasCursorMessage = !string.IsNullOrWhiteSpace(command.ReadMessageId);
        // ReadAtMs 仅作客户端提示；权威时间由存储按 messageId 从库解析。
        var result = await _store.AdvanceReadCursorAsync(
                command.UserId,
                conversationId,
                readAtMs: null,
                hasCursorMessage ? command.ReadMessageId!.Trim() : null,
                ct)
            .ConfigureAwait(false);

        if (!result.Found)
        {
            return ConversationMarkReadResult.Failed(
                command.RequestId,
                "not_found",
                "会话不存在或当前用户不是成员。");
        }

        if (result.Changed)
            _outboxSignal.Notify();

        return ConversationMarkReadResult.Success(
            command.RequestId,
            conversationId,
            result.UnreadCount,
            result.LastReadMessageId,
            result.LastReadAtMs,
            result.Changed);
    }

    private static ConversationMarkReadResult? Validate(ConversationMarkReadCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return ConversationMarkReadResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (command.UserId <= 0)
            return ConversationMarkReadResult.Failed(
                command.RequestId,
                "invalid_user_id",
                "用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.ConversationId)
            || command.ConversationId.Length > ConversationId.MaxLength)
        {
            return ConversationMarkReadResult.Failed(
                command.RequestId,
                "invalid_conversation_id",
                "会话编号无效。");
        }

        var hasMessage = !string.IsNullOrWhiteSpace(command.ReadMessageId);
        var hasTime = command.ReadAtMs.HasValue;
        // 允许：两者皆空（推进到最新）；或仅提供 / 同时提供 ReadMessageId。
        // 禁止：仅有 ReadAtMs 而无 messageId。
        if (hasTime && !hasMessage)
        {
            return ConversationMarkReadResult.Failed(
                command.RequestId,
                "invalid_cursor",
                "已读游标必须提供消息编号，或全部省略以标记到最新消息。");
        }

        if (command.ReadAtMs is <= 0 || command.ReadMessageId?.Length > 64)
            return ConversationMarkReadResult.Failed(
                command.RequestId,
                "invalid_cursor",
                "已读游标无效。");

        return null;
    }
}

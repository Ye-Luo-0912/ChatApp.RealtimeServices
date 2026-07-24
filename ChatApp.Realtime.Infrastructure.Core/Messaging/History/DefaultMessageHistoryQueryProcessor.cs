using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging.History;

public sealed class DefaultMessageHistoryQueryProcessor : IMessageHistoryQueryProcessor
{
    public const int DefaultPageSize = 50;
    public const int MaximumPageSize = 100;
    public const int MaximumResponseBytes = RealtimeWireLimits.MaximumResponseBytes;

    private readonly IRealtimeMessageHistoryStore _store;
    private readonly IRealtimeAttachmentStore _attachmentStore;
    private readonly IRealtimeReactionStore _reactionStore;

    public DefaultMessageHistoryQueryProcessor(
        IRealtimeMessageHistoryStore store,
        IRealtimeAttachmentStore attachmentStore,
        IRealtimeReactionStore reactionStore)
    {
        _store = store;
        _attachmentStore = attachmentStore;
        _reactionStore = reactionStore;
    }

    public async Task<MessageHistoryPage> ProcessAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default)
    {
        var validationError = Validate(query);
        if (validationError is not null)
            return validationError;

        if (!string.IsNullOrWhiteSpace(query.MessageId))
        {
            var message = await _store.TryGetByIdAsync(query.MessageId.Trim(), ct).ConfigureAwait(false);
            if (message is null)
                return MessageHistoryPage.Failed(query.RequestId, "not_found", "消息不存在。");

            if (message.SenderUserId != query.UserId && message.ReceiverUserId != query.UserId)
            {
                var conversationId = message.ConversationId;
                if (string.IsNullOrWhiteSpace(conversationId)
                    || !ConversationId.IsGroup(conversationId)
                    || !await _store.IsConversationMemberAsync(query.UserId, conversationId, ct)
                        .ConfigureAwait(false))
                {
                    return MessageHistoryPage.Failed(query.RequestId, "forbidden", "无权查看该消息。");
                }
            }

            var enrichedSingle = await RealtimeHistoryAttachmentEnricher
                .EnrichAsync(_attachmentStore, [message], ct)
                .ConfigureAwait(false);
            enrichedSingle = await RealtimeHistoryReactionEnricher
                .EnrichAsync(_reactionStore, enrichedSingle, query.UserId, ct)
                .ConfigureAwait(false);
            var single = MessageHistoryPage.Success(query.RequestId, enrichedSingle, null, false);
            if (MeasureUtf8Json(single) > MaximumResponseBytes)
            {
                return MessageHistoryPage.Failed(
                    query.RequestId,
                    "message_too_large",
                    "单条历史消息序列化后超过协议预算。");
            }

            return single;
        }

        var pageSize = Math.Clamp(
            query.Limit == 0 ? DefaultPageSize : query.Limit,
            1,
            MaximumPageSize);

        var hasAfter = query.AfterReceivedAtMs.HasValue;
        IReadOnlyList<RealtimeHistoryMessage> rows;
        if (!string.IsNullOrWhiteSpace(query.ConversationId))
        {
            var conversationId = query.ConversationId.Trim();
            ConversationMessageHistoryResult history;
            if (hasAfter)
            {
                history = await _store.QueryByConversationAfterAsync(
                        query.UserId,
                        conversationId,
                        query.AfterReceivedAtMs!.Value,
                        query.AfterMessageId!.Trim(),
                        pageSize + 1,
                        ct)
                    .ConfigureAwait(false);
            }
            else
            {
                history = await _store.QueryByConversationAsync(
                        query.UserId,
                        conversationId,
                        query.BeforeReceivedAtMs,
                        query.BeforeMessageId,
                        pageSize + 1,
                        ct)
                    .ConfigureAwait(false);
            }

            if (!history.IsMember)
            {
                return MessageHistoryPage.Failed(
                    query.RequestId,
                    "forbidden",
                    "无权查看该会话历史。");
            }

            rows = history.Messages;
        }
        else
        {
            rows = await _store.QueryAsync(
                    query.UserId,
                    query.BeforeReceivedAtMs,
                    query.BeforeMessageId,
                    pageSize + 1,
                    ct)
                .ConfigureAwait(false);
        }

        rows = await RealtimeHistoryAttachmentEnricher
            .EnrichAsync(_attachmentStore, rows, ct)
            .ConfigureAwait(false);
        rows = await RealtimeHistoryReactionEnricher
            .EnrichAsync(_reactionStore, rows, query.UserId, ct)
            .ConfigureAwait(false);

        return PackByActualUtf8Json(query.RequestId, rows, pageSize);
    }

    /// <summary>
    /// 按实际 UTF-8 JSON 字节打包，预算使用 <see cref="RealtimeWireLimits.PackingBudgetBytes"/>。
    /// 单条超过 <see cref="MaximumResponseBytes"/> → <c>message_too_large</c>；
    /// 单条超过 packing 余量但仍 ≤ 硬上限 → 单独返回该条并置 HasMore。
    /// </summary>
    private static MessageHistoryPage PackByActualUtf8Json(
        string requestId,
        IReadOnlyList<RealtimeHistoryMessage> rows,
        int pageSize)
    {
        var items = new List<RealtimeHistoryMessage>(Math.Min(pageSize, rows.Count));
        foreach (var row in rows)
        {
            if (items.Count >= pageSize)
                break;

            items.Add(row);
            var candidate = BuildPackedPage(requestId, items, rows.Count > items.Count);
            var bytes = MeasureUtf8Json(candidate);

            if (bytes <= RealtimeWireLimits.PackingBudgetBytes)
                continue;

            if (items.Count == 1)
            {
                if (bytes > MaximumResponseBytes)
                {
                    return MessageHistoryPage.Failed(
                        requestId,
                        "message_too_large",
                        "单条历史消息序列化后超过协议预算。");
                }

                break;
            }

            items.RemoveAt(items.Count - 1);
            break;
        }

        var hasMore = rows.Count > items.Count;
        return BuildPackedPage(requestId, items, hasMore);
    }

    private static MessageHistoryPage BuildPackedPage(
        string requestId,
        IReadOnlyList<RealtimeHistoryMessage> items,
        bool hasMore)
    {
        var last = items.Count == 0 ? null : items[^1];
        var nextCursor = hasMore && last is not null
            ? new MessageHistoryCursor(last.ReceivedAtMs, last.MessageId)
            : null;
        return MessageHistoryPage.Success(requestId, items, nextCursor, hasMore);
    }

    private static int MeasureUtf8Json(MessageHistoryPage page)
    {
        var json = JsonSerializer.Serialize(
            page,
            RealtimeJsonSerializerContext.Default.MessageHistoryPage);
        return Encoding.UTF8.GetByteCount(json);
    }

    private static MessageHistoryPage? Validate(MessageHistoryQuery query)
    {
        if (string.IsNullOrWhiteSpace(query.RequestId) || query.RequestId.Length > 64)
            return MessageHistoryPage.Failed(
                query.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (query.UserId <= 0)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_user_id",
                "用户编号必须大于 0。");
        if (query.Limit < 0)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_limit",
                "分页大小不能小于 0。");

        if (!string.IsNullOrWhiteSpace(query.ConversationId)
            && query.ConversationId.Length > ConversationId.MaxLength)
        {
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_conversation_id",
                "会话编号无效。");
        }

        if (!string.IsNullOrWhiteSpace(query.MessageId))
        {
            if (query.MessageId.Length > 64)
                return MessageHistoryPage.Failed(query.RequestId, "invalid_message_id", "消息编号无效。");
            return null;
        }

        var hasBeforeTime = query.BeforeReceivedAtMs.HasValue;
        var hasBeforeMessage = !string.IsNullOrWhiteSpace(query.BeforeMessageId);
        if (hasBeforeTime != hasBeforeMessage)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "Before 游标时间和消息编号必须同时提供。");

        var hasAfterTime = query.AfterReceivedAtMs.HasValue;
        var hasAfterMessage = !string.IsNullOrWhiteSpace(query.AfterMessageId);
        if (hasAfterTime != hasAfterMessage)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "After 游标时间和消息编号必须同时提供。");

        if (hasBeforeTime && hasAfterTime)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "Before 与 After 游标不能同时使用。");

        if (hasAfterTime && string.IsNullOrWhiteSpace(query.ConversationId))
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "After 游标仅支持按会话查询。");

        if (query.BeforeReceivedAtMs is <= 0 || query.BeforeMessageId?.Length > 64)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "历史消息游标无效。");
        if (query.AfterReceivedAtMs is <= 0 || query.AfterMessageId?.Length > 64)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "历史消息游标无效。");

        return null;
    }
}

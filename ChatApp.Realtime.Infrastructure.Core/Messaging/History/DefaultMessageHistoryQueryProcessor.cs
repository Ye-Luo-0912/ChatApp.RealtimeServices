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

        // P0-3：所有列表查询必须带 ConversationId。空 ConversationId 的用户级全量历史
        // 已废弃（群消息 receiver_user_id=0 导致全局查询遗漏群消息）。
        if (string.IsNullOrWhiteSpace(query.ConversationId))
        {
            return MessageHistoryPage.Failed(
                query.RequestId,
                "conversation_id_required",
                "历史列表查询必须指定会话编号。");
        }

        var hasAfter = query.AfterChangedAtMs.HasValue;
        var targetConversationId = query.ConversationId.Trim();
        ConversationMessageHistoryResult history;
        if (hasAfter)
        {
            history = await _store.QueryByConversationAfterAsync(
                    query.UserId,
                    targetConversationId,
                    query.AfterChangedAtMs!.Value,
                    query.AfterMessageId!.Trim(),
                    pageSize + 1,
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            history = await _store.QueryByConversationAsync(
                    query.UserId,
                    targetConversationId,
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

        var rows = history.Messages;
        rows = await RealtimeHistoryAttachmentEnricher
            .EnrichAsync(_attachmentStore, rows, ct)
            .ConfigureAwait(false);
        rows = await RealtimeHistoryReactionEnricher
            .EnrichAsync(_reactionStore, rows, query.UserId, ct)
            .ConfigureAwait(false);

        return PackByActualUtf8Json(query.RequestId, rows, pageSize, hasAfter);
    }

    /// <summary>
    /// 按实际 UTF-8 JSON 字节打包，预算使用 <see cref="RealtimeWireLimits.PackingBudgetBytes"/>。
    /// 单条超过 <see cref="MaximumResponseBytes"/> → <c>message_too_large</c>；
    /// 单条超过 packing 余量但仍 ≤ 硬上限 → 单独返回该条并置 HasMore。
    /// </summary>
    /// <remarks>
    /// Perf-4：旧实现每加入一条消息就重新序列化整个 Page，导致 O(N²) 序列化开销。
    /// 新实现先逐条序列化获取精确字节（O(N)），线性累加判断预算，最后仅做一次完整序列化验证。
    /// </remarks>
    private static MessageHistoryPage PackByActualUtf8Json(
        string requestId,
        IReadOnlyList<RealtimeHistoryMessage> rows,
        int pageSize,
        bool isAfterMode)
    {
        // O(N)：逐条序列化获取精确字节大小。
        var perItemBytes = new int[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            perItemBytes[i] = MeasureSingleMessageUtf8Json(rows[i]);
        }

        // 页面 JSON 结构开销估算：
        // {"requestId":"...","items":[...],"nextCursor":{...},"hasMore":true,"succeeded":true,...}
        // 每条 item 在数组中额外 1 字节逗号分隔。nextCursor 约 200 字节。
        const int pageWrapperOverhead = 256;
        const int cursorOverhead = 200;
        const int itemSeparatorOverhead = 1;

        var budget = RealtimeWireLimits.PackingBudgetBytes - pageWrapperOverhead - cursorOverhead;

        var items = new List<RealtimeHistoryMessage>(Math.Min(pageSize, rows.Count));
        var accumulated = 0;
        for (var i = 0; i < rows.Count; i++)
        {
            if (items.Count >= pageSize)
                break;

            var itemSize = perItemBytes[i] + (items.Count > 0 ? itemSeparatorOverhead : 0);

            if (items.Count > 0 && accumulated + itemSize > budget)
                break;

            items.Add(rows[i]);
            accumulated += itemSize;
        }

        var hasMore = rows.Count > items.Count;
        var page = BuildPackedPage(requestId, items, hasMore, isAfterMode);

        // 一次完整序列化验证：估算可能偏低（cursor 实际大小、JSON 转义差异），
        // 超预算则移除最后一条并重新构建（最多重试一次，仍 O(N)）。
        var finalBytes = MeasureUtf8Json(page);
        if (finalBytes > RealtimeWireLimits.PackingBudgetBytes && items.Count > 1)
        {
            items.RemoveAt(items.Count - 1);
            hasMore = rows.Count > items.Count;
            page = BuildPackedPage(requestId, items, hasMore, isAfterMode);
            finalBytes = MeasureUtf8Json(page);
        }

        if (items.Count == 1 && finalBytes > MaximumResponseBytes)
        {
            return MessageHistoryPage.Failed(
                requestId,
                "message_too_large",
                "单条历史消息序列化后超过协议预算。");
        }

        return page;
    }

    private static int MeasureSingleMessageUtf8Json(RealtimeHistoryMessage message)
    {
        var json = JsonSerializer.Serialize(
            message,
            RealtimeJsonSerializerContext.Default.RealtimeHistoryMessage);
        return Encoding.UTF8.GetByteCount(json);
    }

    private static MessageHistoryPage BuildPackedPage(
        string requestId,
        IReadOnlyList<RealtimeHistoryMessage> items,
        bool hasMore,
        bool isAfterMode)
    {
        var last = items.Count == 0 ? null : items[^1];
        MessageHistoryCursor? nextCursor = null;
        if (hasMore && last is not null)
        {
            // P0-2：After 模式按 changed_at_ms 排序，游标必须携带变更水位；
            // Before 模式按 received_at_ms 排序，游标携带接收时间。
            nextCursor = isAfterMode
                ? new MessageHistoryCursor(last.ReceivedAtMs, last.MessageId, last.ChangedAtMs)
                : new MessageHistoryCursor(last.ReceivedAtMs, last.MessageId);
        }
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

        var hasAfterTime = query.AfterChangedAtMs.HasValue;
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
        if (query.AfterChangedAtMs is <= 0 || query.AfterMessageId?.Length > 64)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "历史消息游标无效。");

        return null;
    }
}

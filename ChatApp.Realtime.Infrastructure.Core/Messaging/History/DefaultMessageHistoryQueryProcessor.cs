using System.Text;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging.History;

public sealed class DefaultMessageHistoryQueryProcessor : IMessageHistoryQueryProcessor
{
    public const int DefaultPageSize = 50;
    public const int MaximumPageSize = 100;
    public const int MaximumResponseBytes = 256 * 1024;

    private readonly IRealtimeMessageHistoryStore _store;

    public DefaultMessageHistoryQueryProcessor(IRealtimeMessageHistoryStore store)
    {
        _store = store;
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
                return MessageHistoryPage.Failed(query.RequestId, "forbidden", "无权查看该消息。");

            return MessageHistoryPage.Success(query.RequestId, [message], null, false);
        }

        var pageSize = Math.Clamp(
            query.Limit == 0 ? DefaultPageSize : query.Limit,
            1,
            MaximumPageSize);
        var rows = await _store.QueryAsync(
                query.UserId,
                query.BeforeReceivedAtMs,
                query.BeforeMessageId,
                pageSize + 1,
                ct)
            .ConfigureAwait(false);

        var items = new List<RealtimeHistoryMessage>(Math.Min(pageSize, rows.Count));
        var responseBytes = 0;
        foreach (var row in rows)
        {
            if (items.Count >= pageSize)
                break;

            var itemBytes = EstimateSerializedBytes(row);
            if (items.Count > 0 && responseBytes + itemBytes > MaximumResponseBytes)
                break;

            items.Add(row);
            responseBytes += itemBytes;
        }

        var hasMore = rows.Count > items.Count;
        var last = items.Count == 0 ? null : items[^1];
        var nextCursor = hasMore && last is not null
            ? new MessageHistoryCursor(last.ReceivedAtMs, last.MessageId)
            : null;

        return MessageHistoryPage.Success(
            query.RequestId,
            items,
            nextCursor,
            hasMore);
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

        if (!string.IsNullOrWhiteSpace(query.MessageId))
        {
            if (query.MessageId.Length > 64)
                return MessageHistoryPage.Failed(query.RequestId, "invalid_message_id", "消息编号无效。");
            return null;
        }

        var hasTime = query.BeforeReceivedAtMs.HasValue;
        var hasMessage = !string.IsNullOrWhiteSpace(query.BeforeMessageId);
        if (hasTime != hasMessage)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "游标时间和消息编号必须同时提供。");
        if (query.BeforeReceivedAtMs is <= 0 || query.BeforeMessageId?.Length > 64)
            return MessageHistoryPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "历史消息游标无效。");

        return null;
    }

    private static int EstimateSerializedBytes(RealtimeHistoryMessage item) =>
        128
        + Encoding.UTF8.GetByteCount(item.MessageId)
        + Encoding.UTF8.GetByteCount(item.ClientMessageId)
        + Encoding.UTF8.GetByteCount(item.Content);
}

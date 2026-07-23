using System.Text;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class DefaultConversationListQueryProcessor : IConversationListQueryProcessor
{
    public const int DefaultPageSize = 50;
    public const int MaximumPageSize = 100;
    public const int MaximumResponseBytes = RealtimeWireLimits.MaximumResponseBytes;

    private readonly IRealtimeConversationStore _store;

    public DefaultConversationListQueryProcessor(IRealtimeConversationStore store)
    {
        _store = store;
    }

    public async Task<ConversationListPage> ProcessAsync(
        ConversationListQuery query,
        CancellationToken ct = default)
    {
        var validationError = Validate(query);
        if (validationError is not null)
            return validationError;

        var pageSize = Math.Clamp(
            query.Limit == 0 ? DefaultPageSize : query.Limit,
            1,
            MaximumPageSize);
        var rows = await _store.QueryListAsync(
                query.UserId,
                query.BeforeIsPinned,
                query.BeforePinnedAtMs,
                query.BeforeLastMessageAtMs,
                query.BeforeConversationId,
                pageSize + 1,
                ct)
            .ConfigureAwait(false);

        var items = new List<ConversationListItem>(Math.Min(pageSize, rows.Count));
        var responseBytes = 0;
        foreach (var row in rows)
        {
            if (items.Count >= pageSize)
                break;

            var itemBytes = EstimateSerializedBytes(row);
            if (items.Count > 0 && responseBytes + itemBytes > RealtimeWireLimits.PackingBudgetBytes)
                break;

            items.Add(row);
            responseBytes += itemBytes;
        }

        var hasMore = rows.Count > items.Count;
        var last = items.Count == 0 ? null : items[^1];
        var nextCursor = hasMore && last is not null
            ? new ConversationListCursor(
                last.IsPinned,
                last.PinnedAtMs,
                last.LastMessageAtMs,
                last.ConversationId)
            : null;

        return ConversationListPage.Success(
            query.RequestId,
            items,
            nextCursor,
            hasMore);
    }

    private static ConversationListPage? Validate(ConversationListQuery query)
    {
        if (string.IsNullOrWhiteSpace(query.RequestId) || query.RequestId.Length > 64)
            return ConversationListPage.Failed(
                query.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (query.UserId <= 0)
            return ConversationListPage.Failed(
                query.RequestId,
                "invalid_user_id",
                "用户编号必须大于 0。");
        if (query.Limit < 0)
            return ConversationListPage.Failed(
                query.RequestId,
                "invalid_limit",
                "分页大小不能小于 0。");

        // 四元组游标：全有或全无。PinnedAtMs / LastMessageAtMs 允许为 null（未置顶 / 无消息），
        // 但一旦出现任一游标字段，就必须提供完整集合（IsPinned + ConversationId 必有值）。
        var hasId = !string.IsNullOrWhiteSpace(query.BeforeConversationId);
        var hasPinned = query.BeforeIsPinned.HasValue;
        var hasPinnedAt = query.BeforePinnedAtMs.HasValue;
        var hasLastAt = query.BeforeLastMessageAtMs.HasValue;
        var hasAnyCursorField = hasId || hasPinned || hasPinnedAt || hasLastAt;
        if (hasAnyCursorField && !(hasId && hasPinned))
        {
            return ConversationListPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "会话列表游标必须同时提供置顶状态、置顶时间、最后消息时间与会话编号（时间字段可为 null）。");
        }

        if (hasId
            && (query.BeforeConversationId!.Length > ConversationId.MaxLength
                || query.BeforeLastMessageAtMs is <= 0
                || query.BeforePinnedAtMs is <= 0))
        {
            return ConversationListPage.Failed(
                query.RequestId,
                "invalid_cursor",
                "会话列表游标无效。");
        }

        return null;
    }

    private static int EstimateSerializedBytes(ConversationListItem item) =>
        192
        + Encoding.UTF8.GetByteCount(item.ConversationId)
        + Encoding.UTF8.GetByteCount(item.LastMessageId ?? string.Empty)
        + Encoding.UTF8.GetByteCount(item.LastMessagePreview ?? string.Empty);
}

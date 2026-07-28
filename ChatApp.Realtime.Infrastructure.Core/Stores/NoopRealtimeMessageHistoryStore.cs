using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeMessageHistoryStore : IRealtimeMessageHistoryStore
{
    public Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法查询历史消息。");
    }

    public Task<ConversationMessageHistoryResult> QueryByConversationAsync(
        long userId,
        string conversationId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法按会话查询历史消息。");
    }

    public Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
        long userId,
        string conversationId,
        long afterChangedAtMs,
        string afterMessageId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法按会话向前查询历史消息。");
    }

    public Task<bool> IsConversationMemberAsync(
        long userId,
        string conversationId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法校验会话成员。");
    }

    public Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
        long userId,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法批量校验会话成员。");
    }

    public Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
        long userId,
        IReadOnlyList<HistoryCatchUpQuery> queries,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法批量查询会话补偿历史。");
    }

    public Task<RealtimeHistoryMessage?> TryGetByIdAsync(
        long userId, string messageId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法按 Id 查询消息。");
    }

    public Task<bool> CanAccessMessageAsync(
        long userId, string messageId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法校验消息访问权限。");
    }

    public Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
        IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法解析同步水位。");
    }
}

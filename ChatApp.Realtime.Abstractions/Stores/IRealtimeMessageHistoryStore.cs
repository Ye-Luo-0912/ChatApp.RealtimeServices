using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeMessageHistoryStore
{
    Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default);

    /// <summary>
    /// 按会话 keyset 查询；同一 SQL 内校验 <paramref name="userId"/> 成员身份。
    /// 非成员时 <see cref="ConversationMessageHistoryResult.IsMember"/> 为 false。
    /// </summary>
    Task<ConversationMessageHistoryResult> QueryByConversationAsync(
        long userId,
        string conversationId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default);

    /// <summary>
    /// 按会话向前（更新）keyset 查询；同一 SQL 内校验成员身份。用于重连补偿。
    /// </summary>
    Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
        long userId,
        string conversationId,
        long afterReceivedAtMs,
        string afterMessageId,
        int take,
        CancellationToken ct = default);

    Task<bool> IsConversationMemberAsync(
        long userId,
        string conversationId,
        CancellationToken ct = default);

    /// <summary>
    /// 批量过滤：仅返回 <paramref name="conversationIds"/> 中当前用户确为成员的会话 Id。
    /// </summary>
    Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
        long userId,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct = default);

    /// <summary>
    /// 批量拉取多会话 catch-up（同步引导）。
    /// 每个会话在同一 SQL 内校验 <paramref name="userId"/> 成员身份；非成员返回空列表。
    /// </summary>
    Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
        long userId,
        IReadOnlyList<HistoryCatchUpQuery> queries,
        CancellationToken ct = default);

    /// <summary>按消息 Id 读取单条（审核证据等）；不存在返回 null。</summary>
    Task<RealtimeHistoryMessage?> TryGetByIdAsync(string messageId, CancellationToken ct = default);

    /// <summary>
    /// 将客户端同步水位解析为可增量 catch-up 的真实消息，或标记失效。
    /// 消息不存在/已删 → <see cref="SyncWatermarkInvalidationKind.MessageNotFound"/>（After*/Tip* 为 tip 提示，若有）；
    /// 超前 tip → <see cref="SyncWatermarkInvalidationKind.AheadOfTip"/>；
    /// 无 tip 时仍返回该会话并标记 MessageNotFound（tip 提示为空）。
    /// 处理器将 InvalidationKind 映射为协议层 <c>SyncCursorResetReason</c>；Store 不依赖协议枚举。
    /// 不再静默 tip-clamp 伪装成“已同步”。
    /// </summary>
    Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
        IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
        CancellationToken ct = default);
}

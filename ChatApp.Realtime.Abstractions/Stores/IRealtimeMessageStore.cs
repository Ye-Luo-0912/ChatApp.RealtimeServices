namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 实时消息存储接口，定义了保存实时消息的方法。
/// 该接口的实现类负责将实时消息记录持久化到指定的数据存储中。
/// </summary>
public interface IRealtimeMessageStore
{
    /// <summary>
    /// 将实时消息记录异步保存到数据存储中。
    /// </summary>
    /// <param name="message">要保存的实时消息记录。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <param name="eventToPublish">与消息在同一事务写入 Outbox 的事件。</param>
    /// <returns>原子持久化结果。</returns>
    Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);

    Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);

    /// <summary>
    /// 按批删除该用户作为发送方或接收方的全部消息，直到无剩余行为止；
    /// 并尽力清理该用户相关的 Outbox 行（按 typed 列 <c>target_user_id</c> 精确匹配，
    /// 但保留 <c>AccountCleanupCompleted</c> 完成回传，避免重试抹掉待发布完成事件）。
    /// 已清理过的用户再次调用是安全的（返回 0）。
    /// </summary>
    /// <returns>累计删除的消息行数。</returns>
    Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default);

    /// <summary>
    /// 将事件写入事务 Outbox（<c>ON CONFLICT DO NOTHING</c> / 幂等 EventId），
    /// 供 Outbox Worker 持久发布；不直接走 best-effort 总线。
    /// </summary>
    Task EnqueueEventAsync(
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);
}

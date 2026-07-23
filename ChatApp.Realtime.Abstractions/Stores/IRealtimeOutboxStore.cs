namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeOutboxStore
{
    Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
        string instanceId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    Task MarkPublishedAsync(RealtimeOutboxRecord record, CancellationToken ct = default);

    Task MarkFailedAsync(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan retryDelay,
        CancellationToken ct = default);

    /// <summary>
    /// 将认领中的事件标记为死信（不再自动重试）。
    /// </summary>
    Task MarkDeadAsync(
        RealtimeOutboxRecord record,
        string error,
        CancellationToken ct = default);

    /// <summary>
    /// 将死信事件重置为 Pending，供运维重放。返回是否找到并重置。
    /// </summary>
    Task<bool> ReplayDeadAsync(string eventId, CancellationToken ct = default);

    /// <summary>
    /// 批量将死信重置为 Pending。单次 <c>UPDATE ... WHERE event_id = ANY(...)</c>，
    /// 返回实际重置的 event_id 列表。
    /// </summary>
    Task<IReadOnlyList<string>> ReplayDeadBatchAsync(
        IReadOnlyList<string> eventIds,
        CancellationToken ct = default);

    /// <summary>
    /// 删除已发布且早于 cutoff 的 Outbox 行（分区友好的批量删除）。
    /// </summary>
    Task<int> CleanupPublishedAsync(
        long publishedBeforeMs,
        int batchSize,
        CancellationToken ct = default);

    /// <summary>
    /// 运维/低频对账用：仅聚合 Pending + Dead（走部分索引），不扫 Published 全表。
    /// 热路径指标应由进程内计数器维护，勿高频调用。
    /// </summary>
    Task<RealtimeOutboxStats> GetStatsAsync(CancellationToken ct = default);

    /// <summary>运维查询：按状态/用户分页列出 Outbox 行。</summary>
    Task<IReadOnlyList<RealtimeOutboxListItem>> ListAsync(
        RealtimeOutboxStatus? status,
        long? targetUserId,
        int offset,
        int limit,
        CancellationToken ct = default);

    Task<RealtimeOutboxListItem?> TryGetAsync(string eventId, CancellationToken ct = default);
}

public sealed record RealtimeOutboxStats(
    long PendingCount,
    long? OldestPendingAtMs,
    int MaxAttemptCount,
    long DeadCount = 0,
    long? OldestInFlightAtMs = null);

public sealed record RealtimeOutboxListItem(
    string EventId,
    RealtimeOutboxStatus Status,
    short EventType,
    long TargetUserId,
    int AttemptCount,
    long CreatedAtMs,
    long NextAttemptAtMs,
    long? PublishedAtMs,
    string? LockedBy,
    long? LockedUntilMs,
    string? LastError);

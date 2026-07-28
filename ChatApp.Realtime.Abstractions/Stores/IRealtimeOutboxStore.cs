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
    /// 批量续租已认领的 Outbox 记录的 lease。用 claim_token 校验所有权，
    /// 仅续租仍处于 Pending 且 claim_token 匹配的记录，防止续租已被其他实例认领的记录。
    /// 返回实际续租的记录数。
    /// </summary>
    Task<int> ExtendLeaseBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        TimeSpan leaseExtension,
        CancellationToken ct = default);

    /// <summary>
    /// 批量将认领中的事件标记为已发布。用 UNNEST 配对 event_id + claim_token 校验所有权，
    /// 单次 UPDATE 完成，避免逐事件数据库往返。返回实际命中记录数。
    /// </summary>
    Task<int> MarkPublishedBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        CancellationToken ct = default);

    /// <summary>
    /// 批量将认领中的事件标记为失败（待重试）。每条携带各自的 error 和 retryDelay。
    /// 返回实际命中记录数。
    /// </summary>
    Task<int> MarkFailedBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error, TimeSpan RetryDelay)> failures,
        CancellationToken ct = default);

    /// <summary>
    /// 批量将认领中的事件标记为死信。每条携带各自的 error。返回实际命中记录数。
    /// </summary>
    Task<int> MarkDeadBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error)> deadLetters,
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
    /// Perf-8：列出早于 cutoff 的 Dead 行（用于归档），按 created_at_ms 升序、LIMIT 限定。
    /// 返回的行包含完整 payload 与元数据，调用方归档成功后再调用 <see cref="DeleteDeadBatchAsync"/>。
    /// </summary>
    Task<IReadOnlyList<DeadOutboxRow>> ListDeadAsync(
        long createdBeforeMs,
        int limit,
        CancellationToken ct = default);

    /// <summary>
    /// Perf-8：按 event_id 批量删除 Dead 行。仅当归档成功（或选择跳过归档）后调用。
    /// </summary>
    Task<int> DeleteDeadBatchAsync(
        IReadOnlyList<string> eventIds,
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
    long[]? TargetUserIds,
    int AttemptCount,
    long CreatedAtMs,
    long NextAttemptAtMs,
    long? PublishedAtMs,
    string? LockedBy,
    long? LockedUntilMs,
    string? LastError);

/// <summary>
/// Perf-8：Dead 行归档所需的最小字段集合。归档接收器据此写入对象存储/审计库。
/// </summary>
public sealed record DeadOutboxRow(
    string EventId,
    short EventType,
    long TargetUserId,
    long[]? TargetUserIds,
    int AttemptCount,
    long CreatedAtMs,
    long NextAttemptAtMs,
    string? LastError,
    string PayloadJson);

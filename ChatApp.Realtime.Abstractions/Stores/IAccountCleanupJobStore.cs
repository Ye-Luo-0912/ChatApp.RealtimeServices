namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 账号清理 Saga 作业存储：按 (user_id, phase) 跟踪清理进度，支持断点续跑。
/// </summary>
public interface IAccountCleanupJobStore
{
    /// <summary>
    /// 为指定用户创建初始清理作业（pending, phase=attachments）。
    /// 幂等：若 (user_id, phase=attachments) 已存在则不覆盖，返回已有作业。
    /// </summary>
    Task<AccountCleanupJob> EnqueueJobAsync(
        long userId,
        long occurredAtMs,
        CancellationToken ct = default);

    /// <summary>
    /// 原子地将 pending 作业标记为 running 并返回；若已被其他实例 claim 或无 pending 作业则返回 <c>null</c>。
    /// </summary>
    Task<AccountCleanupJob?> TryClaimAsync(long userId, CancellationToken ct = default);

    /// <summary>
    /// 更新指定 (user_id, phase) 的进度（cursor + status），并刷新 updated_at_ms。
    /// </summary>
    Task UpdateProgressAsync(
        long userId,
        string phase,
        string? cursor,
        string status,
        CancellationToken ct = default);

    /// <summary>
    /// 标记指定 phase 完成，并将下一 phase（若有）置为 pending。
    /// <para>
    /// attachments → metadata(pending)；metadata → completed(pending)。
    /// </para>
    /// </summary>
    Task CompletePhaseAsync(long userId, string phase, CancellationToken ct = default);

    /// <summary>
    /// 获取下一个 pending 作业（按 updated_at_ms 升序），原子地标记为 running 并返回。
    /// 无 pending 作业时返回 <c>null</c>。
    /// </summary>
    Task<AccountCleanupJob?> GetNextPendingAsync(CancellationToken ct = default);

    /// <summary>
    /// 记录一次失败：retry_count++，超过阈值则标记 failed，否则回退为 pending 等待重试。
    /// </summary>
    Task RecordFailureAsync(
        long userId,
        string phase,
        int maxRetryCount,
        CancellationToken ct = default);
}

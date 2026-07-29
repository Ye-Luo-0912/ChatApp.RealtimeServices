using ChatApp.Realtime.Abstractions.Events;

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
    /// 六-1：须提供 <paramref name="claimToken"/> 校验当前 lease 归属，防止旧 lease 误操作。
    /// </summary>
    Task UpdateProgressAsync(
        long userId,
        string phase,
        string? cursor,
        string status,
        string claimToken,
        CancellationToken ct = default);

    /// <summary>
    /// 标记指定 phase 完成，并将下一 phase（若有）置为 pending。
    /// 六-1：须提供 <paramref name="claimToken"/> 校验当前 lease 归属，并清空 lease 字段。
    /// <para>
    /// attachments → metadata(pending)；metadata → completed(pending)。
    /// </para>
    /// <para>
    /// P0-7：使用 UPDATE ... RETURNING 校验 claim_token。返回 <c>false</c> 表示 lease 已丢失
    /// （claim_token 不匹配或状态非 running），此时不创建下一 phase，调用方应停止处理。
    /// </para>
    /// </summary>
    /// <returns><c>true</c> 表示成功推进到下一阶段；<c>false</c> 表示 lease 已丢失。</returns>
    Task<bool> CompletePhaseAsync(
        long userId,
        string phase,
        string claimToken,
        CancellationToken ct = default);

    /// <summary>
    /// P0-7：达到单周期批次上限时，主动将作业回退为 pending（带 claim_token 校验），
    /// 以便下一周期重新认领继续处理。lease 字段清空。
    /// <para>
    /// 若 claim_token 不匹配（lease 已丢失），UPDATE 0 行，返回 <c>false</c>。
    /// </para>
    /// </summary>
    /// <returns><c>true</c> 表示已成功回退 pending；<c>false</c> 表示 lease 已丢失。</returns>
    Task<bool> ReleaseToPendingAsync(
        long userId,
        string phase,
        string claimToken,
        CancellationToken ct = default);

    /// <summary>
    /// 六-1：获取下一个可认领的作业（pending 或 lease 过期的 running），原子地标记为 running、
    /// 写入租约（claim_token / locked_by / locked_until_ms）并返回。
    /// 无可认领作业时返回 <c>null</c>。
    /// </summary>
    /// <param name="instanceId">当前实例标识，写入 locked_by。</param>
    /// <param name="leaseDuration">租约时长；到期后 running 作业可被其他实例重新认领。</param>
    Task<AccountCleanupJob?> GetNextPendingAsync(
        string instanceId,
        TimeSpan leaseDuration,
        CancellationToken ct = default);

    /// <summary>
    /// 六-1：续租当前认领的作业。必须提供正确的 <paramref name="claimToken"/>，
    /// 防止旧 lease 误续新 lease。租约过期或被抢占时返回 <c>false</c>。
    /// </summary>
    Task<bool> RenewLeaseAsync(
        long userId,
        string phase,
        string claimToken,
        TimeSpan leaseExtension,
        CancellationToken ct = default);

    /// <summary>
    /// 六-3：在同一事务中原子地完成 attachments 批次的三个操作：
    /// <list type="number">
    /// <item>写入 purge Outbox 事件（<c>ON CONFLICT (event_id) DO NOTHING</c> 幂等）</item>
    /// <item>删除本批附件元数据（<c>DELETE WHERE attachment_id = ANY(...)</c>）</item>
    /// <item>更新 Job cursor（带 <paramref name="claimToken"/> 校验）</item>
    /// </list>
    /// 任一失败则整体回滚。若 lease 已失效（cursor 更新 0 行）则回滚并返回 <c>false</c>。
    /// </summary>
    /// <param name="userId">目标用户 ID。</param>
    /// <param name="claimToken">当前租约令牌。</param>
    /// <param name="lastAttachmentId">本批最后一条 attachment_id，作为新 cursor。</param>
    /// <param name="attachmentIds">本批待删除的 attachment_id 列表。</param>
    /// <param name="purgeEvent">预构造的 purge Outbox 事件（含稳定 EventId）。</param>
    /// <returns><c>true</c> 表示 cursor 已推进；<c>false</c> 表示 lease 失效需停止处理。</returns>
    Task<bool> ProcessAttachmentsBatchAtomicAsync(
        long userId,
        string claimToken,
        string lastAttachmentId,
        IReadOnlyList<string> attachmentIds,
        RealtimeEvent purgeEvent,
        CancellationToken ct = default);

    /// <summary>
    /// 记录一次失败：retry_count++，超过阈值则标记 failed，否则回退为 pending 等待重试。
    /// 六-1：须提供 <paramref name="claimToken"/> 校验当前 lease 归属，并清空 lease 字段。
    /// </summary>
    Task RecordFailureAsync(
        long userId,
        string phase,
        string claimToken,
        int maxRetryCount,
        CancellationToken ct = default);
}

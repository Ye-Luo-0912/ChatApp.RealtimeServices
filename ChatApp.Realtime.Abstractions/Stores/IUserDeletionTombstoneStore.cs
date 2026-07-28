namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 用户生命周期屏障 + 删除 tombstone 存储。
/// <para>
/// 账号删除事件处理时先写入 tombstone（PK=user_id，幂等，state=Deleting），再执行消息/附件清理。
/// 清理完成后将 state 升级为 Deleted。所有写入处理器（入站消息、Reaction、编辑、撤回、群操作）
/// 必须在处理前检查用户状态，拒绝 Deleting/Deleted 用户的命令。
/// </para>
/// <para>
/// Tombstone 保留期应不少于幂等账本保留期（由 IdempotencyGCWorker 统一清理）。
/// </para>
/// </summary>
public interface IUserDeletionTombstoneStore
{
    /// <summary>
    /// 检查用户是否已注销（Deleting 或 Deleted）。PK 查询，O(1)。
    /// </summary>
    Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default);

    /// <summary>
    /// 查询用户生命周期状态。无 tombstone 行时返回 Active。
    /// </summary>
    Task<UserLifecycleState> GetLifecycleStateAsync(long userId, CancellationToken ct = default);

    /// <summary>
    /// 批量查询多个用户的生命周期状态。
    /// <para>
    /// 单条 SQL（user_id = ANY(@user_ids)）一次取回所有目标用户的状态。
    /// 未在 tombstone 表中找到的用户视为 Active；tombstone 表中 state=Deleting/Deleted 的用户返回对应状态。
    /// 返回字典对每个输入 userId（去重后）包含一个条目。
    /// </para>
    /// <para>
    /// 注意：本方法不获取 advisory lock，仅做读检查。适用于 Create/AddMembers 等群操作
    /// 在事务外快速批量过滤目标用户；事务内的强一致校验仍需走 advisory lock 路径。
    /// </para>
    /// </summary>
    Task<IReadOnlyDictionary<long, UserLifecycleState>> BatchGetUserLifecycleStateAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default);

    /// <summary>
    /// 记录用户删除开始（state=Deleting）。幂等（PK 冲突视为成功）。
    /// 应在账号删除清理开始前调用，确保旧命令在清理过程中也能被拒绝。
    /// </summary>
    Task RecordDeletionAsync(
        long userId,
        string deletionEventId,
        long deletedAtMs,
        CancellationToken ct = default);

    /// <summary>
    /// 标记账号删除清理完成（state=Deleted）。
    /// 清理完成后调用，使观测层能区分"清理中"与"已删除"。
    /// </summary>
    Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default);

    /// <summary>
    /// 清理早于 cutoff 的 tombstone。由 IdempotencyGCWorker 周期调用。
    /// </summary>
    Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default);
}

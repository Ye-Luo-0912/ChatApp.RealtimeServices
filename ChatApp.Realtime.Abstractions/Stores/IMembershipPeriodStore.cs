using System.Data.Common;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 群成员 membership periods 存储。
/// <para>
/// 记录每次入群/离群的时间段，用于精确控制历史可见性。
/// 重新入群后不能查看缺席期间的消息，需要依据此表过滤可见时间段。
/// </para>
/// <para>
/// 事务内写入方法（<see cref="RecordJoinInTransactionAsync"/> /
/// <see cref="RecordLeaveInTransactionAsync"/>）复用调用方已有的连接与事务，
/// 保证 membership period 记录与群操作业务变更同生共死。
/// </para>
/// </summary>
public interface IMembershipPeriodStore
{
    /// <summary>
    /// 记录成员加入群（在群操作事务内调用）。
    /// <para>
    /// 使用 ON CONFLICT DO NOTHING 确保幂等：同一 (conversation_id, user_id, joined_at_ms)
    /// 重复写入不会报错。
    /// </para>
    /// </summary>
    Task RecordJoinInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long joinedAtMs,
        CancellationToken ct = default);

    /// <summary>
    /// 批量记录成员加入（在群操作事务内调用，用 UNNEST 单条 SQL）。
    /// <para>
    /// 用于群创建 / 批量加成员等场景，避免逐成员往返。
    /// 使用 ON CONFLICT DO NOTHING 确保幂等：同一 (conversation_id, user_id, joined_at_ms)
    /// 重复写入不会报错。
    /// </para>
    /// </summary>
    Task RecordJoinsBatchInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long joinedAtMs,
        IReadOnlyList<long> userIds,
        CancellationToken ct = default);

    /// <summary>
    /// 记录成员离群（在群操作事务内调用）。
    /// <para>
    /// 仅更新 <c>left_at_ms IS NULL</c> 的记录（当前活跃时间段），已关闭的时间段不受影响。
    /// </para>
    /// </summary>
    /// <param name="leftReason">离群原因（leave / removed / dissolved）。</param>
    Task RecordLeaveInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long leftAtMs,
        string leftReason,
        CancellationToken ct = default);

    /// <summary>
    /// 查询用户在指定会话中的所有 membership periods（按 joined_at_ms 排序）。
    /// 用于历史查询时过滤可见时间段。
    /// </summary>
    Task<IReadOnlyList<MembershipPeriod>> GetMembershipPeriodsAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default);

    /// <summary>
    /// 六-4：账号清理时删除该用户的全部 membership periods，返回已删除行数。
    /// </summary>
    Task<int> DeleteByUserAsync(long userId, CancellationToken ct = default);
}

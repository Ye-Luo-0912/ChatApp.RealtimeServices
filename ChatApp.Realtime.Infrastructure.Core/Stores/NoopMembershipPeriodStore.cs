using System.Data.Common;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// Noop membership periods 存储。测试与未配置 PostgreSQL 时使用。
/// 事务内写入方法为空操作，<see cref="GetMembershipPeriodsAsync"/> 返回空列表。
/// </summary>
public sealed class NoopMembershipPeriodStore : IMembershipPeriodStore
{
    public Task RecordJoinInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long joinedAtMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task RecordJoinsBatchInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long joinedAtMs,
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task RecordLeaveInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conversationId,
        long userId,
        long leftAtMs,
        string leftReason,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<MembershipPeriod>> GetMembershipPeriodsAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<MembershipPeriod>>(Array.Empty<MembershipPeriod>());
    }
}

using System.Data.Common;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// Noop 群操作审计存储。测试与未配置 PostgreSQL 时使用。
/// RecordAsync / RecordInTransactionAsync 均为空操作。
/// </summary>
public sealed class NoopGroupOperationAuditStore(ILogger<NoopGroupOperationAuditStore> logger)
    : IGroupOperationAuditStore
{
    public Task RecordAsync(GroupOperationAuditEntry entry, CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop group audit skipped: actor={ActorUserId}; op={Operation}; request={RequestId}",
            entry.ActorUserId,
            entry.Operation,
            entry.RequestId);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Noop 事务内审计记录。空操作，复用调用方连接/事务但不写入。
    /// </summary>
    public Task RecordInTransactionAsync(
        GroupOperationAuditEntry entry,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct = default)
    {
        logger.LogDebug(
            "Noop group audit (in-transaction) skipped: actor={ActorUserId}; op={Operation}; request={RequestId}",
            entry.ActorUserId,
            entry.Operation,
            entry.RequestId);
        return Task.CompletedTask;
    }
}

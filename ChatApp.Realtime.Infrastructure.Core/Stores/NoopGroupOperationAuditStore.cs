using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// Noop 群操作审计存储。测试与未配置 PostgreSQL 时使用。
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
}

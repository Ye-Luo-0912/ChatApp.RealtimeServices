using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 通话审计的内存存储。只记录参与者、状态、时间、失败分类与 QoE 汇总。
/// </summary>
public sealed class InMemoryCallAuditStore : ICallAuditStore
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<CallAuditEntry>> _entries =
        new(StringComparer.Ordinal);

    public Task RecordAsync(CallAuditEntry entry, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var queue = _entries.GetOrAdd(entry.CallId, static _ => new ConcurrentQueue<CallAuditEntry>());
        queue.Enqueue(entry);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<CallAuditEntry>> ListAsync(string callId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (!_entries.TryGetValue(callId, out var queue))
            return Task.FromResult<IReadOnlyList<CallAuditEntry>>(Array.Empty<CallAuditEntry>());

        var result = queue.ToArray();
        // 倒序：最新在前。
        Array.Reverse(result);
        return Task.FromResult<IReadOnlyList<CallAuditEntry>>(result);
    }
}
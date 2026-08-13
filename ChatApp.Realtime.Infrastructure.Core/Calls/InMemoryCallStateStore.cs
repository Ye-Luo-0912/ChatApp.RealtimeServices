using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 通话临时状态的内存存储。以 call id 为键加锁，按 revision 做 CAS 原子迁移，
/// 并应用 TTL。只保存控制元数据，绝不保存 SDP/ICE 载荷。
/// </summary>
public sealed class InMemoryCallStateStore : ICallStateStore
{
    private readonly ConcurrentDictionary<string, Record> _records = new(StringComparer.Ordinal);
    private readonly TimeProvider _clock;

    public InMemoryCallStateStore(TimeProvider? clock = null)
    {
        _clock = clock ?? TimeProvider.System;
    }

    public Task<CallStateSnapshot?> GetAsync(string callId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (!_records.TryGetValue(callId, out var record))
            return Task.FromResult<CallStateSnapshot?>(null);

        // 过期记录按安全网清理（处理器通常已显式超时处理）。
        if (_clock.GetUtcNow().ToUnixTimeMilliseconds() >= record.Snapshot.ExpiresAtMs)
        {
            _records.TryRemove(callId, out _);
            return Task.FromResult<CallStateSnapshot?>(null);
        }

        return Task.FromResult<CallStateSnapshot?>(record.Snapshot);
    }

    public Task<CallStateSnapshot?> CompareAndSwapAsync(
        string callId,
        long? expectedRevision,
        CallStateSnapshot candidate,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var expiresAt = _clock.GetUtcNow().ToUnixTimeMilliseconds() + (long)ttl.TotalMilliseconds;
        var record = new Record(candidate with { ExpiresAtMs = expiresAt });

        if (expectedRevision is null)
        {
            // 创建：要求当前不存在（或已过期清理）。
            bool created = false;
            Record? prior = null;
            _records.AddOrUpdate(
                callId,
                _ =>
                {
                    created = true;
                    return record;
                },
                (_, existing) =>
                {
                    prior = existing;
                    return existing;
                });
            return Task.FromResult<CallStateSnapshot?>(created ? record.Snapshot : null);
        }

        var expected = expectedRevision.Value;
        var outcome = _records.AddOrUpdate(
            callId,
            _ => record, // 不应发生；若键消失则写入（视为创建成功语义弱化，但 CAS 语义下调用方会重查）。
            (_, existing) =>
            {
                if (existing.Snapshot.Revision != expected)
                    return existing; // 冲突：保留现有。
                return record;
            });

        // 成功：写入后的快照即为候选（revision == candidate.Revision）。
        // 冲突：保留现有快照（revision == expected != candidate.Revision）。
        return Task.FromResult<CallStateSnapshot?>(
            outcome.Snapshot.Revision == candidate.Revision ? outcome.Snapshot : null);
    }

    public Task RemoveAsync(string callId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _records.TryRemove(callId, out _);
        return Task.CompletedTask;
    }

    private sealed record Record(CallStateSnapshot Snapshot);
}
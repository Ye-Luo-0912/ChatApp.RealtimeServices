using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Perf-8：Outbox Dead 行的归档接收器。在物理删除前落盘到对象存储/审计库等冷存储。
/// 实现应做到至少一次（at-least-once）：归档返回成功后，调用方才会删除 PostgreSQL 行。
/// 同一 event_id 可能被归档多次（worker 重启后重新列出），实现应做幂等处理（例如按 event_id 覆盖）。
/// </summary>
public interface IDeadLetterArchiveSink
{
    /// <summary>接收器名称，与 <c>OutboxOptions.DeadArchiveSink</c> 配置匹配。</summary>
    string Name { get; }

    /// <summary>
    /// 批量归档 Dead 行。返回实际归档成功的 event_id 列表；调用方仅删除这些行。
    /// 任意行失败不应抛出异常，而是不包含在返回列表中（下一周期会重试）。
    /// </summary>
    Task<IReadOnlyList<string>> ArchiveAsync(
        IReadOnlyList<DeadOutboxRow> rows,
        CancellationToken ct = default);
}

/// <summary>
/// Perf-8：默认实现的空归档接收器。不落盘任何数据，直接返回全部 event_id 表示"归档成功"。
/// 用于未配置外部对象存储/审计库时，仍允许 Dead 行按 TTL 物理删除。
/// </summary>
public sealed class NullDeadLetterArchiveSink : IDeadLetterArchiveSink
{
    public string Name => "null";

    public Task<IReadOnlyList<string>> ArchiveAsync(
        IReadOnlyList<DeadOutboxRow> rows,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (rows.Count == 0)
            return Task.FromResult<IReadOnlyList<string>>([]);

        var ids = new List<string>(rows.Count);
        foreach (var row in rows)
        {
            if (!string.IsNullOrEmpty(row.EventId))
                ids.Add(row.EventId);
        }
        return Task.FromResult<IReadOnlyList<string>>(ids);
    }
}

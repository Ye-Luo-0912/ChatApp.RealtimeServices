using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.State;

namespace ChatApp.Realtime.Infrastructure.State;

public sealed class InMemoryRealtimeStateStore : IRealtimeStateStore
{
    private readonly ConcurrentDictionary<string, string> _values = new(StringComparer.Ordinal);

    /// <summary>
    /// 将给定的键值对异步存储到内存状态存储中。
    /// </summary>
    /// <param name="key">要存储的数据的键。</param>
    /// <param name="value">与指定键关联的值。</param>
    /// <param name="ct">用于传播通知，指示操作应被取消。</param>
    /// <returns>表示异步操作的任务。</returns>
    public Task SetAsync(string key, string value, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values[key] = value;
        return Task.CompletedTask;
    }

    /// <summary>
    /// 从内存状态存储中异步获取与给定键关联的值。
    /// </summary>
    /// <param name="key">要检索的数据的键。</param>
    /// <param name="ct">用于传播通知，指示操作应被取消。</param>
    /// <returns>表示异步操作的任务，结果为与指定键关联的值；如果找不到该键，则返回null。</returns>
    public Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values.TryGetValue(key, out var value);
        return Task.FromResult(value);
    }

    /// <summary>
    /// 从内存状态存储中异步移除指定键的键值对。
    /// </summary>
    /// <param name="key">要移除的数据的键。</param>
    /// <param name="ct">用于传播通知，指示操作应被取消。</param>
    /// <returns>表示异步操作的任务。</returns>
    public Task RemoveAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values.TryRemove(key, out _);
        return Task.CompletedTask;
    }
}

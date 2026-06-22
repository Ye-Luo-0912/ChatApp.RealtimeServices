using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.State;

namespace ChatApp.Realtime.Infrastructure.Core.State;

public sealed class InMemoryRealtimeStateStore : IRealtimeStateStore
{
    private readonly ConcurrentDictionary<string, string> _values = new(StringComparer.Ordinal);

    public Task SetAsync(string key, string value, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values[key] = value;
        return Task.CompletedTask;
    }

    public Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values.TryGetValue(key, out var value);
        return Task.FromResult(value);
    }

    public Task RemoveAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values.TryRemove(key, out _);
        return Task.CompletedTask;
    }
}

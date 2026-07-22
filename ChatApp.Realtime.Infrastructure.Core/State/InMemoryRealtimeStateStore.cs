using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.State;

namespace ChatApp.Realtime.Infrastructure.Core.State;

public sealed class InMemoryRealtimeStateStore : IRealtimeStateStore
{
    private readonly ConcurrentDictionary<string, StateEntry> _values = new(StringComparer.Ordinal);

    public Task SetAsync(string key, string value, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values[key] = new StateEntry(
            value,
            ttl.HasValue ? DateTimeOffset.UtcNow.Add(ttl.Value).ToUnixTimeMilliseconds() : null);
        return Task.CompletedTask;
    }

    public Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (!_values.TryGetValue(key, out var entry))
            return Task.FromResult<string?>(null);

        if (entry.ExpiresAtMs is not null
            && entry.ExpiresAtMs <= DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
        {
            _values.TryRemove(key, out _);
            return Task.FromResult<string?>(null);
        }

        return Task.FromResult<string?>(entry.Value);
    }

    public Task RemoveAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _values.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    private sealed record StateEntry(string Value, long? ExpiresAtMs);
}

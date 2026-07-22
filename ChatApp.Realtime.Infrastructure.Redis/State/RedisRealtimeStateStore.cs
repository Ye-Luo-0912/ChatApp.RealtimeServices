using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.State;

public sealed class RedisRealtimeStateStore : IRealtimeStateStore
{
    private readonly RealtimeGarnetClient _client;

    public RedisRealtimeStateStore(RealtimeGarnetClient client)
    {
        _client = client;
    }

    public async Task SetAsync(
        string key,
        string value,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var expiration = ttl.HasValue ? new Expiration(ttl.Value) : Expiration.Default;
        await _client.GetDatabase().StringSetAsync(key, value, expiration).WaitAsync(ct).ConfigureAwait(false);
    }

    public async Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var value = await _client.GetDatabase().StringGetAsync(key).WaitAsync(ct).ConfigureAwait(false);
        return value.HasValue ? value.ToString() : null;
    }

    public async Task RemoveAsync(string key, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await _client.GetDatabase().KeyDeleteAsync(key).WaitAsync(ct).ConfigureAwait(false);
    }
}

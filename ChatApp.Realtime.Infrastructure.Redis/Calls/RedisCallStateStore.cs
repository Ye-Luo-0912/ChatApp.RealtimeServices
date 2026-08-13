using System.Text.Json;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Calls;

/// <summary>
/// 通话临时状态存储（Redis/Garnet）。以 call id 为键、revision 为 CAS 前置条件，
/// 配合 TTL 实现有界生命周期。只保存控制元数据，绝不保存 SDP/ICE 载荷。
/// </summary>
public sealed class RedisCallStateStore : ICallStateStore
{
    private const char Separator = '|';
    private const string MissingRevision = "-";

    private readonly RealtimeGarnetClient _client;
    private readonly IDatabase _db;

    // Lua 原子 CAS：键为 "rev|json"。expected='-' 表示要求不存在（创建）。
    private const string CasScript = """
        local key = KEYS[1]
        local current = redis.call('GET', key)
        local expected = ARGV[1]
        local payload = ARGV[2]
        local ttlSec = tonumber(ARGV[3])
        if current == false then
            if expected == '-' then
                redis.call('SET', key, payload, 'EX', ttlSec)
                return payload
            else
                return 0
            end
        end
        local sep = string.find(current, '|')
        if sep == nil then
            return 0
        end
        local curRev = string.sub(current, 1, sep - 1)
        if expected == '-' then
            return 0
        end
        if curRev == expected then
            redis.call('SET', key, payload, 'EX', ttlSec)
            return payload
        else
            return 0
        end
        """;

    public RedisCallStateStore(RealtimeGarnetClient client)
    {
        _client = client;
        _db = client.GetDatabase();
    }

    public async Task<CallStateSnapshot?> GetAsync(string callId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var raw = await _db.StringGetAsync(StateKey(callId)).WaitAsync(ct).ConfigureAwait(false);
        if (raw.IsNull)
            return null;

        return TryDecode(raw.ToString(), out var snapshot) ? snapshot : null;
    }

    public async Task<CallStateSnapshot?> CompareAndSwapAsync(
        string callId,
        long? expectedRevision,
        CallStateSnapshot candidate,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var payload = Encode(candidate);
        var expected = expectedRevision?.ToString() ?? MissingRevision;
        var ttlSec = Math.Max(1, (long)ttl.TotalSeconds);

        var result = await _db.ScriptEvaluateAsync(
                CasScript,
                new RedisKey[] { StateKey(callId) },
                new RedisValue[] { expected, payload, ttlSec })
            .WaitAsync(ct)
            .ConfigureAwait(false);

        if (result.IsNull)
            return null;

        var value = result.ToString();
        if (value == "0")
            return null;

        return TryDecode(value, out var decoded) ? decoded : null;
    }

    public async Task RemoveAsync(string callId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await _db.KeyDeleteAsync(StateKey(callId)).WaitAsync(ct).ConfigureAwait(false);
    }

    private static RedisKey StateKey(string callId) => CallKeys.StateKey(callId);

    private static string Encode(CallStateSnapshot snapshot)
        => snapshot.Revision.ToString() + Separator + JsonSerializer.Serialize(snapshot);

    private static bool TryDecode(string raw, out CallStateSnapshot? snapshot)
    {
        snapshot = null;
        var sep = raw.IndexOf(Separator);
        if (sep <= 0)
            return false;

        var json = raw[(sep + 1)..];
        try
        {
            snapshot = JsonSerializer.Deserialize<CallStateSnapshot>(json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            return snapshot is not null;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
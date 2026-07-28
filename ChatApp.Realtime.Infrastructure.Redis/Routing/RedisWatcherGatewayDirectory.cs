using System.Globalization;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis ZSET + SET 双层结构的 <see cref="IWatcherGatewayDirectory"/> 实现。
/// <para>
/// P0-4：以 (watchedUserId, watcherUserId, instanceId) 三元组为真实成员，确保注册/注销幂等。
/// </para>
/// <para>
/// 两层结构：
/// <list type="bullet">
/// <item>关系明细层 ZSET <c>watchers:{watchedUserId}:instances</c>：
/// member = <c>{watcherUserId}:{instanceId}</c>（唯一标识一个观察关系），
/// score = 租约到期 Unix 毫秒（<see cref="LeaseMs"/> 续期）。</item>
/// <item>路由聚合层 SET <c>watchers:{watchedUserId}:gateways</c>：
/// member = <c>instanceId</c>（去重后的 Gateway 实例 ID）。</item>
/// </list>
/// </para>
/// <para>
/// 添加 watcher 时用 Lua 脚本原子执行 ZADD（关系明细）+ SADD（路由聚合）；
/// 移除 watcher 时用 Lua 脚本原子执行 ZREM，并在该 Gateway 无其他 watcher 时 SREM。
/// </para>
/// <para>
/// 查询时通过 Lua 脚本清理 ZSET 过期成员并同步移除 SET 中已无 watcher 的陈旧 Gateway，
/// 直接返回 SET 中去重后的 Gateway 列表，不再在 C# 中去重。
/// </para>
/// <para>
/// 租约机制保证 Gateway 崩溃后不会残留永久脏路由：未续期的成员在
/// <see cref="LeaseMs"/> 后自动过期，被下一次查询清理。
/// </para>
/// <para>
/// 查询失败时返回空集合（不抛异常），调用方据此回退到广播模式，
/// 同时通过 <see cref="RoutingMetrics"/> 记录失败计数。
/// </para>
/// </summary>
public sealed class RedisWatcherGatewayDirectory : IWatcherGatewayDirectory
{
    /// <summary>
    /// 租约时长（毫秒）。Gateway 必须在此周期内重新注册（心跳）以维持观察关系；
    /// 未续期的成员将被视为陈旧路由并自动清理。
    /// </summary>
    public const long LeaseMs = 300_000; // 5 分钟

    private const string KeyPrefix = "watchers:";
    private const string InstancesKeySuffix = ":instances";
    private const string GatewaysKeySuffix = ":gateways";
    private const char MemberSeparator = ':';

    /// <summary>
    /// P0-9：全局活跃 Gateway shard ZSET 的 key。
    /// <para>
    /// member = instanceId，score = 该实例最近一次 watcher 注册的租约到期 Unix 毫秒。
    /// RegisterWatchersAsync 时同步 ZADD 此 key（续期），ListActiveShardsAsync 时按 score 过期过滤。
    /// 租约机制保证 Gateway 崩溃后此 key 中的陈旧实例自动过期。
    /// </para>
    /// </summary>
    private const string ActiveShardsKey = "watchers:__active_shards__";

    /// <summary>
    /// 添加 watcher 的 Lua 脚本：原子执行 ZADD 关系明细 + SADD 路由聚合。
    /// <para>
    /// KEYS[1] = watchers:{watchedUserId}:instances (ZSET)
    /// KEYS[2] = watchers:{watchedUserId}:gateways (SET)
    /// ARGV[1] = member ({watcherUserId}:{instanceId})
    /// ARGV[2] = expiresAtMs (score)
    /// ARGV[3] = gatewayInstanceId
    /// </para>
    /// </summary>
    private const string AddWatcherScript = @"redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('SADD', KEYS[2], ARGV[3])
return 1";

    /// <summary>
    /// 移除 watcher 的 Lua 脚本：原子执行 ZREM，并在该 Gateway 无其他 watcher 时 SREM 路由聚合。
    /// <para>
    /// KEYS[1] = watchers:{watchedUserId}:instances (ZSET)
    /// KEYS[2] = watchers:{watchedUserId}:gateways (SET)
    /// ARGV[1] = member ({watcherUserId}:{instanceId})
    /// ARGV[2] = gatewayInstanceId
    /// </para>
    /// </summary>
    private const string RemoveWatcherScript = @"redis.call('ZREM', KEYS[1], ARGV[1])
local remaining = redis.call('ZRANGE', KEYS[1], 0, -1)
local hasGateway = false
for _, m in ipairs(remaining) do
  local sep = string.find(m, ':', 1, true)
  if sep and sep < #m then
    if string.sub(m, sep + 1) == ARGV[2] then
      hasGateway = true
      break
    end
  end
end
if not hasGateway then
  redis.call('SREM', KEYS[2], ARGV[2])
end
return 1";

    /// <summary>
    /// 查询 watcher Gateway 的 Lua 脚本：清理 ZSET 过期成员，同步清理 SET 中陈旧 Gateway，返回去重后的 Gateway 列表。
    /// <para>
    /// KEYS[1] = watchers:{watchedUserId}:instances (ZSET)
    /// KEYS[2] = watchers:{watchedUserId}:gateways (SET)
    /// ARGV[1] = nowMs
    /// </para>
    /// </summary>
    private const string QueryWatcherGatewaysScript = @"redis.call('ZREMRANGEBYSCORE', KEYS[1], -1, ARGV[1])
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
local found = {}
for _, m in ipairs(members) do
  local sep = string.find(m, ':', 1, true)
  if sep and sep < #m then
    found[string.sub(m, sep + 1)] = true
  end
end
local gateways = redis.call('SMEMBERS', KEYS[2])
local result = {}
for _, gw in ipairs(gateways) do
  if found[gw] then
    table.insert(result, gw)
  else
    redis.call('SREM', KEYS[2], gw)
  end
end
return result";

    private readonly RealtimeGarnetClient _client;
    private readonly RoutingMetrics _metrics;
    private readonly ILogger<RedisWatcherGatewayDirectory> _logger;

    public RedisWatcherGatewayDirectory(
        RealtimeGarnetClient client,
        RoutingMetrics metrics,
        ILogger<RedisWatcherGatewayDirectory> logger)
    {
        _client = client;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = _client.GetDatabase();
            var instancesKey = FormatInstancesKey(watchedUserId);
            var gatewaysKey = FormatGatewaysKey(watchedUserId);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Lua 脚本原子执行：清理 ZSET 过期成员 -> 检查 SET 中每个 Gateway 是否仍有存活 watcher -> 返回去重后的 Gateway 列表。
            var result = await db.ScriptEvaluateAsync(
                QueryWatcherGatewaysScript,
                new RedisKey[] { instancesKey, gatewaysKey },
                new RedisValue[] { nowMs })
                .WaitAsync(cancellationToken).ConfigureAwait(false);

            return ConvertRedisResultToStrings(result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("watcher", "single");
            _logger.LogWarning(
                ex,
                "Redis watcher 目录查询失败，回退到空集合。被观察用户={WatchedUserId}",
                watchedUserId);
            return Array.Empty<string>();
        }
    }

    public async Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetWatcherGatewaysManyAsync(
        IReadOnlyList<long> watchedUserIds,
        CancellationToken cancellationToken = default)
    {
        if (watchedUserIds is null || watchedUserIds.Count == 0)
            return new Dictionary<long, IReadOnlyList<string>>(0);

        try
        {
            var db = _client.GetDatabase();
            var batch = db.CreateBatch();
            var tasks = new Task<RedisValue[]>[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var gatewaysKey = FormatGatewaysKey(watchedUserIds[i]);
                // 批量内不做过期清理（避免增加 RTT）；直接读取路由聚合 SET 获取去重后的 Gateway 列表。
                tasks[i] = batch.SetMembersAsync(gatewaysKey);
            }
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            var result = new Dictionary<long, IReadOnlyList<string>>(watchedUserIds.Count);
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var members = await tasks[i].ConfigureAwait(false);
                result[watchedUserIds[i]] = ConvertRedisValuesToStrings(members);
            }

            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("watcher", "many");
            _logger.LogWarning(
                ex,
                "Redis watcher 目录批量查询失败，回退到空映射。被观察用户数={Count}",
                watchedUserIds.Count);
            return new Dictionary<long, IReadOnlyList<string>>(0);
        }
    }

    public async Task RegisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watchedUserIds is null || watchedUserIds.Count == 0)
            return;

        try
        {
            var db = _client.GetDatabase();
            var expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + LeaseMs;
            var member = FormatMember(watcherUserId, instanceId);

            // P0-9：批量写入时额外维护全局活跃 shard ZSET，使 LookupFailure 时可枚举所有活跃 Gateway 实例。
            // 同一 instanceId 多次注册仅刷新 score（续期），不产生重复 member。
            var batch = db.CreateBatch();
            var tasks = new Task[watchedUserIds.Count + 1];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var instancesKey = FormatInstancesKey(watchedUserIds[i]);
                var gatewaysKey = FormatGatewaysKey(watchedUserIds[i]);
                // Lua 脚本原子执行 ZADD 关系明细 + SADD 路由聚合。
                tasks[i] = batch.ScriptEvaluateAsync(
                    AddWatcherScript,
                    new RedisKey[] { instancesKey, gatewaysKey },
                    new RedisValue[] { member, expiryMs, instanceId });
            }
            tasks[watchedUserIds.Count] = batch.SortedSetAddAsync(ActiveShardsKey, instanceId, expiryMs);
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("watcher", "register");
            _logger.LogWarning(
                ex,
                "Redis watcher 注册失败。watcher={WatcherUserId}；instance={InstanceId}；被观察用户数={Count}",
                watcherUserId,
                instanceId,
                watchedUserIds.Count);
        }
    }

    /// <summary>
    /// P0-9：列出当前所有已知活跃的 Gateway shard ID。
    /// <para>
    /// 读取 <see cref="ActiveShardsKey"/> ZSET，清理过期成员后返回存活 instanceId 集合。
    /// 查询失败时返回空集合（不抛异常），调用方据此再次回退到广播。
    /// </para>
    /// </summary>
    public async Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = _client.GetDatabase();
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // 先清理过期成员，保持 ZSET 紧凑；失败不阻塞查询。
            try
            {
                await db.SortedSetRemoveRangeByScoreAsync(ActiveShardsKey, start: -1, stop: nowMs)
                    .WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // 清理失败不影响读取。
            }

            var members = await db.SortedSetRangeByScoreAsync(
                ActiveShardsKey,
                start: nowMs + 1,
                stop: double.PositiveInfinity)
                .WaitAsync(cancellationToken).ConfigureAwait(false);

            if (members.Length == 0)
                return Array.Empty<string>();

            var result = new List<string>(members.Length);
            foreach (var member in members)
            {
                if (member.HasValue)
                {
                    var instanceId = (string?)member;
                    if (!string.IsNullOrWhiteSpace(instanceId))
                        result.Add(instanceId);
                }
            }

            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("watcher", "list_active_shards");
            _logger.LogWarning(
                ex,
                "Redis 活跃 shard 列表查询失败，回退到空集合。");
            return Array.Empty<string>();
        }
    }

    public async Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watchedUserIds is null || watchedUserIds.Count == 0)
            return;

        try
        {
            var db = _client.GetDatabase();
            var member = FormatMember(watcherUserId, instanceId);

            var batch = db.CreateBatch();
            var tasks = new Task<RedisResult>[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var instancesKey = FormatInstancesKey(watchedUserIds[i]);
                var gatewaysKey = FormatGatewaysKey(watchedUserIds[i]);
                // Lua 脚本原子执行 ZREM，并在该 Gateway 无其他 watcher 时 SREM 路由聚合。
                tasks[i] = batch.ScriptEvaluateAsync(
                    RemoveWatcherScript,
                    new RedisKey[] { instancesKey, gatewaysKey },
                    new RedisValue[] { member, instanceId });
            }
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("watcher", "unregister");
            _logger.LogWarning(
                ex,
                "Redis watcher 注销失败。watcher={WatcherUserId}；instance={InstanceId}；被观察用户数={Count}",
                watcherUserId,
                instanceId,
                watchedUserIds.Count);
        }
    }

    private static string FormatInstancesKey(long watchedUserId) =>
        string.Concat(KeyPrefix, watchedUserId.ToString(CultureInfo.InvariantCulture), InstancesKeySuffix);

    private static string FormatGatewaysKey(long watchedUserId) =>
        string.Concat(KeyPrefix, watchedUserId.ToString(CultureInfo.InvariantCulture), GatewaysKeySuffix);

    /// <summary>
    /// 构建 ZSET member：<c>{watcherUserId}:{instanceId}</c>。
    /// watcherUserId 为数字，不包含分隔符，因此按首个 <c>:</c> 拆分即可还原 instanceId。
    /// </summary>
    private static string FormatMember(long watcherUserId, string instanceId) =>
        string.Concat(watcherUserId.ToString(CultureInfo.InvariantCulture), MemberSeparator, instanceId);

    /// <summary>
    /// 将 Lua 脚本返回的 <see cref="RedisResult"/> 转换为字符串列表。
    /// </summary>
    private static IReadOnlyList<string> ConvertRedisResultToStrings(RedisResult result)
    {
        if (result.IsNull)
            return Array.Empty<string>();

        var array = (RedisResult[]?)result;
        if (array is null || array.Length == 0)
            return Array.Empty<string>();

        var list = new List<string>(array.Length);
        foreach (var item in array)
        {
            if (!item.IsNull)
            {
                var value = (string?)item;
                if (!string.IsNullOrEmpty(value))
                    list.Add(value);
            }
        }

        return list;
    }

    /// <summary>
    /// 将 <see cref="RedisValue"/> 数组转换为字符串列表。
    /// </summary>
    private static IReadOnlyList<string> ConvertRedisValuesToStrings(RedisValue[] members)
    {
        if (members.Length == 0)
            return Array.Empty<string>();

        var list = new List<string>(members.Length);
        foreach (var member in members)
        {
            if (member.HasValue)
            {
                var value = (string?)member;
                if (!string.IsNullOrEmpty(value))
                    list.Add(value);
            }
        }

        return list;
    }
}

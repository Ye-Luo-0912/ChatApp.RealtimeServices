using System.Globalization;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis ZSET 的 <see cref="IWatcherGatewayDirectory"/> 实现。
/// <para>
/// P0-4：以 (watchedUserId, watcherUserId, instanceId) 三元组为真实成员，确保注册/注销幂等。
/// </para>
/// <para>
/// 每个 watchedUserId 对应一个 ZSET <c>watchers:{watchedUserId}:instances</c>：
/// member = <c>{watcherUserId}:{instanceId}</c>（唯一标识一个观察关系），
/// score = 租约到期 Unix 毫秒（<see cref="LeaseMs"/> 续期）。
/// 注册时 ZADD（幂等：相同 member 仅刷新 score，不产生重复计数）；
/// 注销时 ZREM（幂等：不存在的 member 为无操作）。
/// </para>
/// <para>
/// 查询时先清理过期成员（ZREMRANGEBYSCORE），再按 score 过滤存活成员（ZRANGEBYSCORE），
/// 最后从 member 中提取去重的 instanceId 集合。
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
    private const string KeySuffix = ":instances";
    private const char MemberSeparator = ':';

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
            var key = FormatKey(watchedUserId);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // 先移除已过期成员，保持 ZSET 紧凑；失败不阻塞查询。
            try
            {
                await db.SortedSetRemoveRangeByScoreAsync(key, start: -1, stop: nowMs)
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
                key,
                start: nowMs + 1,
                stop: double.PositiveInfinity)
                .WaitAsync(cancellationToken).ConfigureAwait(false);

            if (members.Length == 0)
                return Array.Empty<string>();

            return ExtractInstanceIds(members);
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
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var batch = db.CreateBatch();
            var tasks = new Task<RedisValue[]>[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                // 批量内不做过期清理（避免增加 RTT）；查询时按 score 过滤已过期成员。
                tasks[i] = batch.SortedSetRangeByScoreAsync(
                    key,
                    start: nowMs + 1,
                    stop: double.PositiveInfinity);
            }
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            var result = new Dictionary<long, IReadOnlyList<string>>(watchedUserIds.Count);
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var members = await tasks[i].ConfigureAwait(false);
                result[watchedUserIds[i]] = members.Length == 0
                    ? Array.Empty<string>()
                    : ExtractInstanceIds(members);
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

            var batch = db.CreateBatch();
            var tasks = new Task[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                // ZADD 幂等：相同 member 仅刷新 score（续期），不产生重复计数。
                tasks[i] = batch.SortedSetAddAsync(key, member, expiryMs);
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
            _metrics.RecordDirectoryLookupFailed("watcher", "register");
            _logger.LogWarning(
                ex,
                "Redis watcher 注册失败。watcher={WatcherUserId}；instance={InstanceId}；被观察用户数={Count}",
                watcherUserId,
                instanceId,
                watchedUserIds.Count);
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
            var tasks = new Task[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                // ZREM 幂等：不存在的 member 为无操作（返回 0）。
                tasks[i] = batch.SortedSetRemoveAsync(key, member);
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

    private static string FormatKey(long watchedUserId) =>
        string.Concat(KeyPrefix, watchedUserId.ToString(CultureInfo.InvariantCulture), KeySuffix);

    /// <summary>
    /// 构建 ZSET member：<c>{watcherUserId}:{instanceId}</c>。
    /// watcherUserId 为数字，不包含分隔符，因此按首个 <c>:</c> 拆分即可还原 instanceId。
    /// </summary>
    private static string FormatMember(long watcherUserId, string instanceId) =>
        string.Concat(watcherUserId.ToString(CultureInfo.InvariantCulture), MemberSeparator, instanceId);

    /// <summary>
    /// 从 ZSET member 列表中提取去重的 instanceId 集合。
    /// member 格式为 <c>{watcherUserId}:{instanceId}</c>，按首个 <c>:</c> 拆分取第二段。
    /// </summary>
    private static IReadOnlyList<string> ExtractInstanceIds(RedisValue[] members)
    {
        if (members.Length == 0)
            return Array.Empty<string>();

        // 用 HashSet 去重：同一 instance 上多个 watcher 会产生多个 member，但对应同一 instanceId。
        var seen = new HashSet<string>(members.Length, StringComparer.Ordinal);
        var result = new List<string>(members.Length);
        foreach (var member in members)
        {
            if (!member.HasValue)
                continue;

            var text = (string?)member;
            if (string.IsNullOrEmpty(text))
                continue;

            var sepIndex = text.IndexOf(MemberSeparator);
            if (sepIndex < 0 || sepIndex >= text.Length - 1)
                continue;

            var instanceId = text[(sepIndex + 1)..];
            if (seen.Add(instanceId))
                result.Add(instanceId);
        }

        return result;
    }
}

using System.Globalization;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis HASH 的 <see cref="IWatcherGatewayDirectory"/> 实现。
/// <para>
/// 每个 watchedUserId 对应一个 HASH <c>watchers:{watchedUserId}:instances</c>：
/// field = instanceId，value = 该实例上观察此 watchedUserId 的 watcher 计数。
/// 注册时 HINCRBY +1，注销时 HINCRBY -1，归零时 HDEL。
/// </para>
/// <para>
/// 查询时 HGETALL 返回所有 value &gt; 0 的 field（即有 watcher 的 Gateway 实例）。
/// </para>
/// <para>
/// 查询失败时返回空集合（不抛异常），调用方据此回退到广播模式，
/// 同时通过 <see cref="RoutingMetrics"/> 记录失败计数。
/// </para>
/// </summary>
public sealed class RedisWatcherGatewayDirectory : IWatcherGatewayDirectory
{
    private const string KeyPrefix = "watchers:";
    private const string KeySuffix = ":instances";

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
            var entries = await db.HashGetAllAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);

            if (entries.Length == 0)
                return Array.Empty<string>();

            var result = new List<string>(entries.Length);
            foreach (var entry in entries)
            {
                if (entry.Value.HasValue
                    && entry.Value.TryParse(out long count)
                    && count > 0
                    && entry.Name.HasValue)
                {
                    var instanceId = (string?)entry.Name;
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
            var tasks = new Task<HashEntry[]>[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                tasks[i] = batch.HashGetAllAsync(key);
            }
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            var result = new Dictionary<long, IReadOnlyList<string>>(watchedUserIds.Count);
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var entries = await tasks[i].ConfigureAwait(false);
                if (entries.Length == 0)
                {
                    result[watchedUserIds[i]] = Array.Empty<string>();
                    continue;
                }

                var list = new List<string>(entries.Length);
                foreach (var entry in entries)
                {
                    if (entry.Value.HasValue
                        && entry.Value.TryParse(out long count)
                        && count > 0
                        && entry.Name.HasValue)
                    {
                        var instanceId = (string?)entry.Name;
                        if (!string.IsNullOrWhiteSpace(instanceId))
                            list.Add(instanceId);
                    }
                }
                result[watchedUserIds[i]] = list;
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
            var batch = db.CreateBatch();
            var tasks = new Task[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                tasks[i] = batch.HashIncrementAsync(key, instanceId, 1);
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
            var batch = db.CreateBatch();
            var tasks = new Task<long>[watchedUserIds.Count];
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var key = FormatKey(watchedUserIds[i]);
                // HINCRBY -1，归零时由后续 HDEL 清理；用返回值判断是否需要删除。
                tasks[i] = batch.HashIncrementAsync(key, instanceId, -1);
            }
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            // 清理归零字段，避免 HASH 无限增长。
            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var remaining = await tasks[i].ConfigureAwait(false);
                if (remaining <= 0)
                {
                    var key = FormatKey(watchedUserIds[i]);
                    await db.HashDeleteAsync(key, instanceId).WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
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
}

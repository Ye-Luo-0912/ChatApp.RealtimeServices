using System.Globalization;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis ZSET 的 <see cref="IGatewayDirectory"/> 实现。
/// <para>
/// 读取与 IGlobalPresenceStore 相同的 ZSET <c>presence:{userId}:instances</c>：
/// member = GatewayInstanceId，score = ExpiresAtUnixMs。
/// 查询时过滤掉已过期（score &lt;= now）的成员。
/// </para>
/// <para>
/// 查询失败时返回空集合（不抛异常），调用方据此回退到广播模式，
/// 同时通过 <see cref="RoutingMetrics"/> 记录失败计数。
/// </para>
/// </summary>
public sealed class RedisGatewayDirectory : IGatewayDirectory
{
    private const string KeyPrefix = "presence:";
    private const string KeySuffix = ":instances";

    private readonly RealtimeGarnetClient _client;
    private readonly RoutingMetrics _metrics;
    private readonly ILogger<RedisGatewayDirectory> _logger;

    public RedisGatewayDirectory(
        RealtimeGarnetClient client,
        RoutingMetrics metrics,
        ILogger<RedisGatewayDirectory> logger)
    {
        _client = client;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await QueryOnlineGatewaysCoreAsync(userId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "single");
            _logger.LogWarning(
                ex,
                "Redis 网关目录查询失败，回退到空集合。用户={UserId}",
                userId);
            return Array.Empty<string>();
        }
    }

    public async Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        if (userIds is null || userIds.Count == 0)
            return new Dictionary<long, IReadOnlyList<string>>(0);

        try
        {
            return await QueryOnlineGatewaysManyCoreAsync(userIds, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "many");
            _logger.LogWarning(
                ex,
                "Redis 网关目录批量查询失败，回退到空映射。用户数={Count}",
                userIds.Count);
            var empty = new Dictionary<long, IReadOnlyList<string>>(0);
            return empty;
        }
    }

    /// <summary>
    /// P0-9：带状态的查询。区分"查询成功但用户离线"与"查询失败"，
    /// 使发布方能够在失败时枚举所有活跃 shards 分别发布，避免分片模式下广播 fallback 无人消费。
    /// </summary>
    public async Task<GatewayLookupResult> GetOnlineGatewaysWithStatusAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var gateways = await QueryOnlineGatewaysCoreAsync(userId, cancellationToken)
                .ConfigureAwait(false);
            var kind = gateways.Count > 0
                ? GatewayLookupResultKind.Success
                : GatewayLookupResultKind.UserOffline;
            return new GatewayLookupResult(kind, gateways);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "single");
            _logger.LogWarning(
                ex,
                "Redis 网关目录查询失败，返回 LookupFailure。用户={UserId}",
                userId);
            return new GatewayLookupResult(
                GatewayLookupResultKind.LookupFailure,
                Array.Empty<string>());
        }
    }

    /// <summary>
    /// P0-9：批量带状态查询。整批失败返回 <see cref="GatewayLookupResultKind.LookupFailure"/>。
    /// </summary>
    public async Task<GatewayLookupManyResult> GetOnlineGatewaysManyWithStatusAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        if (userIds is null || userIds.Count == 0)
            return new GatewayLookupManyResult(
                GatewayLookupResultKind.Success,
                new Dictionary<long, IReadOnlyList<string>>(0));

        try
        {
            var map = await QueryOnlineGatewaysManyCoreAsync(userIds, cancellationToken)
                .ConfigureAwait(false);
            return new GatewayLookupManyResult(
                GatewayLookupResultKind.Success,
                map);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "many");
            _logger.LogWarning(
                ex,
                "Redis 网关目录批量查询失败，返回 LookupFailure。用户数={Count}",
                userIds.Count);
            return new GatewayLookupManyResult(
                GatewayLookupResultKind.LookupFailure,
                new Dictionary<long, IReadOnlyList<string>>(0));
        }
    }

    /// <summary>
    /// 单用户查询核心逻辑（不捕获异常），供 <see cref="GetOnlineGatewaysAsync"/> 与
    /// <see cref="GetOnlineGatewaysWithStatusAsync"/> 复用，保证 Redis 查询逻辑一致。
    /// </summary>
    private async Task<IReadOnlyList<string>> QueryOnlineGatewaysCoreAsync(
        long userId,
        CancellationToken cancellationToken)
    {
        var db = _client.GetDatabase();
        var key = FormatKey(userId);
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

        var entries = await db.SortedSetRangeByScoreAsync(
            key,
            start: nowMs + 1,
            stop: double.PositiveInfinity)
            .WaitAsync(cancellationToken).ConfigureAwait(false);

        if (entries.Length == 0)
            return Array.Empty<string>();

        var result = new List<string>(entries.Length);
        foreach (var entry in entries)
        {
            if (entry.HasValue)
            {
                var instanceId = (string?)entry;
                if (!string.IsNullOrWhiteSpace(instanceId))
                    result.Add(instanceId);
            }
        }

        return result;
    }

    /// <summary>
    /// 批量查询核心逻辑（不捕获异常），供 <see cref="GetOnlineGatewaysManyAsync"/> 与
    /// <see cref="GetOnlineGatewaysManyWithStatusAsync"/> 复用。
    /// </summary>
    private async Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> QueryOnlineGatewaysManyCoreAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken)
    {
        var db = _client.GetDatabase();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = db.CreateBatch();
        var tasks = new Task<RedisValue[]>[userIds.Count];
        for (var i = 0; i < userIds.Count; i++)
        {
            var key = FormatKey(userIds[i]);
            // 批量内不做过期清理（避免增加 RTT）；查询时按 score 过滤已过期成员。
            tasks[i] = batch.SortedSetRangeByScoreAsync(
                key,
                start: nowMs + 1,
                stop: double.PositiveInfinity);
        }
        batch.Execute();
        await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

        var result = new Dictionary<long, IReadOnlyList<string>>(userIds.Count);
        for (var i = 0; i < userIds.Count; i++)
        {
            var entries = await tasks[i].ConfigureAwait(false);
            if (entries.Length == 0)
            {
                result[userIds[i]] = Array.Empty<string>();
                continue;
            }

            var list = new List<string>(entries.Length);
            foreach (var entry in entries)
            {
                if (entry.HasValue)
                {
                    var instanceId = (string?)entry;
                    if (!string.IsNullOrWhiteSpace(instanceId))
                        list.Add(instanceId);
                }
            }
            result[userIds[i]] = list;
        }

        return result;
    }

    /// <summary>
    /// 五-1：账号删除时显式清理该用户的在线路由 ZSET。
    /// <para>
    /// DEL <c>presence:{userId}:instances</c>，使该用户立即从 Gateway 路由中消失。
    /// 失败仅记录日志，不抛异常，不阻塞账号清理 Saga。
    /// </para>
    /// </summary>
    public async Task PurgeUserRoutingAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var db = _client.GetDatabase();
            var key = FormatKey(userId);
            await db.KeyDeleteAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Redis 网关目录用户路由清理失败，不阻塞清理流程。用户={UserId}",
                userId);
        }
    }

    private static string FormatKey(long userId) =>
        string.Concat(KeyPrefix, userId.ToString(CultureInfo.InvariantCulture), KeySuffix);
}

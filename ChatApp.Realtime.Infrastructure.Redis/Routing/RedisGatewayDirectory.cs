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

    private static string FormatKey(long userId) =>
        string.Concat(KeyPrefix, userId.ToString(CultureInfo.InvariantCulture), KeySuffix);
}

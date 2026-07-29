using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis ZSET + HASH 引用计数的 <see cref="IConversationGatewayDirectory"/> 实现。
/// <para>
/// Perf-2：会话级受众路由目录。维护两个 key：
/// <list type="bullet">
/// <item>ZSET <c>conversation_audience:{conversationId}:instances</c>：member = GatewayInstanceId，score = 心跳到期 Unix 毫秒（租约）。</item>
/// <item>HASH <c>conversation_audience:{conversationId}:refs</c>：field = GatewayInstanceId，value = 该实例上的会话 audience session 计数。</item>
/// </list>
/// </para>
/// <para>
/// P0-4：引用计数模型修复了"同一 Gateway 上多个用户在同一群，一个离线后 ZREM 移除整个 gateway，
/// 其余用户收不到消息"的缺陷。RegisterConversationAsync 增加引用计数，UnregisterConversationAsync
/// 减少引用计数，仅当计数归零时才从 ZSET 移除 Gateway 实例。
/// </para>
/// <para>
/// Gateway 在用户上线时把本实例加入该用户所有群会话的 audience ZSET，定期心跳续期；
/// 用户离线或 Gateway 崩溃后，过期成员在下次查询时被自动清理。
/// </para>
/// <para>
/// 查询时先 ZREMRANGEBYSCORE 清理过期成员，再按 score 过滤存活成员，一次返回该会话所有在线
/// Gateway 实例集合，替代逐用户查询 N 个 <c>presence:{userId}:instances</c> keys。
/// </para>
/// <para>
/// 查询失败时返回 <see cref="GatewayLookupResultKind.LookupFailure"/>，调用方回退到 per-user 路由。
/// </para>
/// </summary>
public sealed class RedisConversationGatewayDirectory : IConversationGatewayDirectory
{
    private const string KeyPrefix = "conversation_audience:";
    private const string InstancesSuffix = ":instances";
    private const string RefsSuffix = ":refs";

    private readonly RealtimeGarnetClient _client;
    private readonly RoutingMetrics _metrics;
    private readonly ILogger<RedisConversationGatewayDirectory> _logger;

    public RedisConversationGatewayDirectory(
        RealtimeGarnetClient client,
        RoutingMetrics metrics,
        ILogger<RedisConversationGatewayDirectory> logger)
    {
        _client = client;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task<GatewayLookupResult> GetConversationGatewaysAsync(
        string conversationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);

        try
        {
            var db = _client.GetDatabase();
            var key = FormatInstancesKey(conversationId);
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
            {
                // 会话无在线成员（正常情况，不投递）。
                return new GatewayLookupResult(
                    GatewayLookupResultKind.UserOffline,
                    Array.Empty<string>());
            }

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

            return new GatewayLookupResult(
                result.Count > 0
                    ? GatewayLookupResultKind.Success
                    : GatewayLookupResultKind.UserOffline,
                result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "conversation");
            _logger.LogWarning(
                ex,
                "Redis 会话受众目录查询失败，返回 LookupFailure。会话={ConversationId}",
                conversationId);
            return new GatewayLookupResult(
                GatewayLookupResultKind.LookupFailure,
                Array.Empty<string>());
        }
    }

    /// <summary>
    /// Gateway 在用户加入会话 audience 时注册本实例。
    /// <para>
    /// P0-4：引用计数模型。HINCRBY 增加 HASH <c>:refs</c> 中的 session 计数，
    /// ZADD 维护 ZSET <c>:instances</c> 中的租约到期时间。
    /// 同一 Gateway 上多个用户加入同一会话时，引用计数 > 1，单个用户离线不会移除 Gateway。
    /// </para>
    /// </summary>
    public async Task RegisterConversationAsync(
        string conversationId,
        string gatewayInstanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(gatewayInstanceId);

        try
        {
            var db = _client.GetDatabase();
            var instancesKey = FormatInstancesKey(conversationId);
            var refsKey = FormatRefsKey(conversationId);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var expiryMs = nowMs + (long)leaseDuration.TotalMilliseconds;

            // P0-4：先增加引用计数，再刷新租约。
            // 即使引用计数操作与租约操作非原子，租约到期后的 ZREMRANGEBYSCORE 仍可清理，
            // 而后续 Register 会重新建立引用计数 + 租约。
            await db.HashIncrementAsync(refsKey, gatewayInstanceId, 1)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            await db.SortedSetAddAsync(instancesKey, gatewayInstanceId, expiryMs)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "conversation_register");
            _logger.LogWarning(
                ex,
                "Redis 会话受众目录注册失败。会话={ConversationId}；实例={InstanceId}",
                conversationId,
                gatewayInstanceId);
        }
    }

    /// <summary>
    /// Gateway 定期续租会话 audience 成员资格。
    /// <para>
    /// P0-4：仅刷新 ZSET 租约（ZADD 更新 score），不修改引用计数。
    /// 引用计数在 Register/Unregister 时维护，续租只延长租约。
    /// </para>
    /// </summary>
    public async Task RenewConversationLeaseAsync(
        string conversationId,
        string gatewayInstanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(gatewayInstanceId);

        try
        {
            var db = _client.GetDatabase();
            var instancesKey = FormatInstancesKey(conversationId);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var expiryMs = nowMs + (long)leaseDuration.TotalMilliseconds;

            await db.SortedSetAddAsync(instancesKey, gatewayInstanceId, expiryMs)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "conversation_renew");
            _logger.LogWarning(
                ex,
                "Redis 会话受众目录续租失败。会话={ConversationId}；实例={InstanceId}",
                conversationId,
                gatewayInstanceId);
        }
    }

    /// <summary>
    /// Gateway 在用户离开会话 audience 时注销本实例。
    /// <para>
    /// P0-4：引用计数模型。HINCRBY -1 减少 HASH <c>:refs</c> 中的 session 计数，
    /// 仅当计数归零（≤ 0）时才从 ZSET <c>:instances</c> 移除 Gateway 并清理 HASH field。
    /// 这样同一 Gateway 上其他用户的会话 audience 不会被误删。
    /// </para>
    /// </summary>
    public async Task UnregisterConversationAsync(
        string conversationId,
        string gatewayInstanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(gatewayInstanceId);

        try
        {
            var db = _client.GetDatabase();
            var instancesKey = FormatInstancesKey(conversationId);
            var refsKey = FormatRefsKey(conversationId);

            // P0-4：减少引用计数，仅当归零时移除 ZSET 与 HASH field。
            var newCount = await db.HashDecrementAsync(refsKey, gatewayInstanceId, 1)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
            if (newCount <= 0)
            {
                await db.SortedSetRemoveAsync(instancesKey, gatewayInstanceId)
                    .WaitAsync(cancellationToken).ConfigureAwait(false);
                await db.HashDeleteAsync(refsKey, gatewayInstanceId)
                    .WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _metrics.RecordDirectoryLookupFailed("gateway", "conversation_unregister");
            _logger.LogWarning(
                ex,
                "Redis 会话受众目录注销失败。会话={ConversationId}；实例={InstanceId}",
                conversationId,
                gatewayInstanceId);
        }
    }

    private static string FormatInstancesKey(string conversationId) =>
        string.Concat(KeyPrefix, conversationId, InstancesSuffix);

    private static string FormatRefsKey(string conversationId) =>
        string.Concat(KeyPrefix, conversationId, RefsSuffix);
}

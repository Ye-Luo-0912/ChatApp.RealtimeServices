using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Routing;

/// <summary>
/// 基于 Redis ZSET 的 <see cref="IConversationGatewayDirectory"/> 实现。
/// <para>
/// Perf-2：会话级受众路由目录。维护 ZSET <c>conversation_audience:{conversationId}:instances</c>：
/// member = GatewayInstanceId，score = 心跳到期 Unix 毫秒。
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
    private const string KeySuffix = ":instances";

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
            var key = FormatKey(conversationId);
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
    /// 四-3：ZADD <c>conversation_audience:{conversationId}:instances</c>，
    /// member = gatewayInstanceId，score = 租约到期 Unix 毫秒。
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
            var key = FormatKey(conversationId);
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var expiryMs = nowMs + (long)leaseDuration.TotalMilliseconds;

            await db.SortedSetAddAsync(key, gatewayInstanceId, expiryMs)
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
    /// 实现与 <see cref="RegisterConversationAsync"/> 相同：ZADD 会更新既有 member 的 score。
    /// </para>
    /// </summary>
    public Task RenewConversationLeaseAsync(
        string conversationId,
        string gatewayInstanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
        => RegisterConversationAsync(conversationId, gatewayInstanceId, leaseDuration, cancellationToken);

    /// <summary>
    /// Gateway 在用户离开会话 audience 时注销本实例。
    /// <para>
    /// ZREM <c>conversation_audience:{conversationId}:instances</c> 中的 gatewayInstanceId。
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
            var key = FormatKey(conversationId);

            await db.SortedSetRemoveAsync(key, gatewayInstanceId)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
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

    private static string FormatKey(string conversationId) =>
        string.Concat(KeyPrefix, conversationId, KeySuffix);
}

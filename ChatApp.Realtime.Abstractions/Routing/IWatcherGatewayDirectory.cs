namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 被观察用户 -> Gateway 实例路由目录。
/// <para>
/// Presence 事件分片投递的基础设施：查询某用户当前被哪些 Gateway 实例上的 watcher 观察，
/// 供 Ephemeral Presence 发布方按 Gateway 定向投递使用（区别于 <see cref="IGatewayDirectory"/>，
/// 后者描述用户自身在哪些 Gateway 在线）。
/// </para>
/// <para>
/// 实现应以幂等方式记录 (watchedUserId, watcherUserId, instanceId) 三元组，
/// 并能返回某 watchedUserId 当前有 watcher 的全部 Gateway 实例 ID 集合。
/// </para>
/// <para>
/// 查询失败时应返回空集合而非抛异常，调用方据此回退到广播模式。
/// </para>
/// </summary>
public interface IWatcherGatewayDirectory
{
    /// <summary>
    /// 查询指定被观察用户当前有 watcher 的所有 Gateway 实例 ID。
    /// </summary>
    /// <param name="watchedUserId">被观察用户 ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>有 watcher 的 Gateway 实例 ID 集合；无 watcher 或查询失败时返回空集合。</returns>
    Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量查询多个被观察用户当前有 watcher 的所有 Gateway 实例 ID。
    /// </summary>
    /// <param name="watchedUserIds">被观察用户 ID 列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>用户 ID -> 有 watcher 的 Gateway 实例 ID 集合的映射；无 watcher 的用户映射到空集合。</returns>
    Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetWatcherGatewaysManyAsync(
        IReadOnlyList<long> watchedUserIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 注册 watcher：标记 <paramref name="instanceId"/> 上的 <paramref name="watcherUserId"/> 正在观察指定的被观察用户集合。
    /// <para>
    /// 实现必须幂等：对相同 (watchedUserId, watcherUserId, instanceId) 重复注册不应产生重复计数。
    /// </para>
    /// </summary>
    /// <param name="watcherUserId">观察者用户 ID。</param>
    /// <param name="watchedUserIds">被观察用户 ID 列表。</param>
    /// <param name="instanceId">watcher 所在 Gateway 实例 ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    Task RegisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 注销 watcher：移除 <paramref name="instanceId"/> 上的 <paramref name="watcherUserId"/> 对指定被观察用户的观察关系。
    /// <para>
    /// 实现必须幂等：注销不存在的观察关系应为无操作。
    /// </para>
    /// </summary>
    /// <param name="watcherUserId">观察者用户 ID。</param>
    /// <param name="watchedUserIds">被观察用户 ID 列表。</param>
    /// <param name="instanceId">watcher 所在 Gateway 实例 ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 列出当前所有已知活跃的 Gateway shard ID（基于 watcher 注册关系维护的全局活跃集合）。
    /// <para>
    /// P0-9：用于 <see cref="IGatewayDirectory"/> 查询失败时枚举所有活跃 Gateway shards，
    /// 分别发布到各自 shard subject，避免分片模式下广播 fallback 无人消费。
    /// </para>
    /// <para>
    /// 实现应返回最近仍在注册（租约未过期）的 Gateway 实例 ID 集合。
    /// 查询失败时返回空集合而非抛异常，调用方据此再次回退到广播。
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>活跃 Gateway 实例 ID 集合；无活跃实例或查询失败时返回空集合。</returns>
    Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default);
}

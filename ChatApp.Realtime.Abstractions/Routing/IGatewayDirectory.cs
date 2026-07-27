namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 用户 -> Gateway 实例在线路由目录。
/// <para>
/// 第三阶段大规模路由的基础设施：查询某用户当前在哪些 Gateway 实例上在线，
/// 供 Realtime Event / Ephemeral 事件按 Gateway 分片投递使用。
/// </para>
/// <para>
/// 实现应读取与 <c>IGlobalPresenceStore</c> 相同的 Redis ZSET
/// （<c>presence:{userId}:instances</c>，member = GatewayInstanceId，score = ExpiresAtUnixMs），
/// 过滤掉已过期的成员后返回有效的实例 ID 集合。
/// </para>
/// <para>
/// 查询失败时应返回空集合而非抛异常，调用方据此回退到广播模式。
/// </para>
/// </summary>
public interface IGatewayDirectory
{
    /// <summary>
    /// 查询指定用户当前在线的所有 Gateway 实例 ID。
    /// </summary>
    /// <param name="userId">目标用户 ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>在线 Gateway 实例 ID 集合；无在线实例或查询失败时返回空集合。</returns>
    Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
        long userId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量查询多个用户当前在线的所有 Gateway 实例 ID。
    /// </summary>
    /// <param name="userIds">目标用户 ID 列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>用户 ID -> 在线 Gateway 实例 ID 集合的映射；无在线实例的用户映射到空集合。</returns>
    Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 查询指定用户当前在线的所有 Gateway 实例 ID，并返回查询状态。
    /// <para>
    /// P0-9：与 <see cref="GetOnlineGatewaysAsync"/> 的区别在于能够区分
    /// "查询成功但用户离线"（<see cref="GatewayLookupResultKind.UserOffline"/>）
    /// 与"查询失败"（<see cref="GatewayLookupResultKind.LookupFailure"/>）。
    /// 前者不投递，后者需要枚举所有活跃 shards 分别发布。
    /// </para>
    /// <para>
    /// 默认实现可基于 <see cref="GetOnlineGatewaysAsync"/> 返回
    /// <see cref="GatewayLookupResultKind.LookupFailure"/>（无法区分时按失败处理，保证不丢事件）。
    /// </para>
    /// </summary>
    /// <param name="userId">目标用户 ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>查询结果，包含状态分类与 Gateway 实例 ID 集合。</returns>
    Task<GatewayLookupResult> GetOnlineGatewaysWithStatusAsync(
        long userId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量查询多个用户当前在线的所有 Gateway 实例 ID，并返回查询状态。
    /// <para>
    /// P0-9：与 <see cref="GetOnlineGatewaysManyAsync"/> 的区别在于能够区分
    /// "查询成功但用户离线"与"查询失败/部分失败"。
    /// </para>
    /// </summary>
    /// <param name="userIds">目标用户 ID 列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>查询结果，包含状态分类与用户 ID -> Gateway 实例 ID 集合的映射。</returns>
    Task<GatewayLookupManyResult> GetOnlineGatewaysManyWithStatusAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default);
}

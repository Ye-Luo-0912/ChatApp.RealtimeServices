namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 会话 -> Gateway 实例在线路由目录。
/// <para>
/// Perf-2：群事件路由优化的核心接口。群聚合事件携带
/// <see cref="AudienceKind.Conversation"/> + ConversationId，
/// Publisher 通过本接口一次查询返回该会话所有在线成员所在的 Gateway 实例集合，
/// 替代逐用户查询 <see cref="IGatewayDirectory"/>（N 次 Redis 查询 → 1 次）。
/// </para>
/// <para>
/// 实现应维护 Redis SET <c>conversation_audience:{conversationId}:instances</c>，
/// member = GatewayInstanceId。Gateway 在用户上线时把本实例加入该用户所有群会话的 audience SET，
/// 用户离线时移除。
/// </para>
/// <para>
/// 查询失败时返回 <see cref="GatewayLookupResultKind.LookupFailure"/>，
/// 调用方据此回退到 <see cref="IGatewayDirectory"/> 的 per-user 路由。
/// </para>
/// </summary>
public interface IConversationGatewayDirectory
{
    /// <summary>
    /// 查询指定会话当前所有在线成员所在的 Gateway 实例 ID 集合。
    /// </summary>
    /// <param name="conversationId">会话编号。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>
    /// 查询结果，包含状态分类与 Gateway 实例 ID 集合。
    /// <see cref="GatewayLookupResultKind.Success"/>：有在线实例；
    /// <see cref="GatewayLookupResultKind.UserOffline"/>：会话无在线成员（正常不投递）；
    /// <see cref="GatewayLookupResultKind.LookupFailure"/>：查询失败，回退到 per-user 路由。
    /// </returns>
    Task<GatewayLookupResult> GetConversationGatewaysAsync(
        string conversationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gateway 在用户加入会话 audience 时注册本实例。
    /// </summary>
    Task RegisterConversationAsync(
        string conversationId,
        string gatewayInstanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gateway 定期续租会话 audience 成员资格。
    /// </summary>
    Task RenewConversationLeaseAsync(
        string conversationId,
        string gatewayInstanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gateway 在用户离开会话 audience 时注销本实例。
    /// </summary>
    Task UnregisterConversationAsync(
        string conversationId,
        string gatewayInstanceId,
        CancellationToken cancellationToken = default);
}

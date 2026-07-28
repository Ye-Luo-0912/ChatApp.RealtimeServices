namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 事件受众类型，决定 Publisher 的路由策略。
/// <para>
/// Perf-2：群聚合事件使用 <see cref="Conversation"/> 受众，Publisher 通过
/// <see cref="IConversationGatewayDirectory"/> 查询会话在线 Gateway 实例集合，
/// 避免 N 次 per-user Redis 查询。
/// </para>
/// <para>
/// 默认值 <see cref="User"/>：按 <see cref="Events.RealtimeEvent.TargetUserId"/> /
/// <see cref="Events.RealtimeEvent.TargetUserIds"/> 路由（兼容历史路径）。
/// </para>
/// </summary>
public enum AudienceKind : byte
{
    /// <summary>
    /// 用户级路由（默认）：按 TargetUserId / TargetUserIds 查询 IGatewayDirectory。
    /// </summary>
    User = 0,

    /// <summary>
    /// 会话级路由：按 ConversationId 查询 IConversationGatewayDirectory，
    /// 一次查询返回该会话所有在线成员所在的 Gateway 实例集合。
    /// </summary>
    Conversation = 1
}

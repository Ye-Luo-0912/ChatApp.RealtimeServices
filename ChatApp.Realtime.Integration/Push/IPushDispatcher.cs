namespace ChatApp.Realtime.Integration.Push;

/// <summary>
/// 离线推送投递端口（RealtimeServices 调用，Gateway 或独立 Push Service 实现）。
/// <para>
/// 实现方职责：
/// <list type="number">
/// <item>从 <c>IPushTokenStore</c> 拉取目标用户的全部活跃推送令牌。</item>
/// <item>按平台选择 Provider（FCM/APNs/WebPush）投递。</item>
/// <item>对无效令牌（Provider 返回 invalid_token）返回指纹，调用方负责注销。</item>
/// <item>实现有界 Retry + DLQ：Provider 限流/不可用时指数退避重试，超限进入 DLQ。</item>
/// <item>实现 Provider 限流：单 Provider QPS 上限，超限排队或拒绝。</item>
/// </item>
/// </para>
/// <para>
/// 实现方不负责：
/// <list type="bullet">
/// <item>在线判断（由 RealtimeServices 在调用前通过 Presence/Gateway 目录判定）。</item>
/// <item>会话静音/Mention 策略（由 RealtimeServices 在构造命令前过滤）。</item>
/// <item>Token 加密存储（由 <c>IPushTokenStore</c> 实现负责）。</item>
/// </list>
/// </para>
/// <para>
/// 幂等性：同一 <see cref="PushDeliveryCommand.MessageId"/> + TargetUserId 的投递应幂等。
/// 实现方应维护最近 N 天的已投递消息 Id 集合（Redis 去重），防止重复推送。
/// </para>
/// </summary>
public interface IPushDispatcher
{
    /// <summary>
    /// 向目标用户投递离线推送。
    /// </summary>
    /// <param name="command">投递命令（已过滤静音/在线状态）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>
    /// 投递结果汇总。用户无注册令牌时返回 <see cref="PushDeliveryResult.NoTokensRegistered"/>。
    /// </returns>
    /// <remarks>
    /// 实现应快速返回（不阻塞消息处理主流程）：
    /// <list type="bullet">
    /// <item>令牌拉取 + Provider 调用应在后台 Task 中执行，本方法返回已受理。</item>
    /// <item>或者本方法同步等待全部 Provider 响应后返回（适用于低 QPS 场景）。</item>
    /// </list>
    /// 实现选择取决于部署拓扑：Gateway 内嵌 vs 独立 Push Service。
    /// </remarks>
    Task<PushDeliveryResult> DispatchAsync(PushDeliveryCommand command, CancellationToken cancellationToken = default);
}

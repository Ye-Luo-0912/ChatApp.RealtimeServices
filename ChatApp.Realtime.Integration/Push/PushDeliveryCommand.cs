namespace ChatApp.Realtime.Integration.Push;

/// <summary>
/// 推送投递命令（RealtimeServices -> Push 投递方）。
/// <para>
/// RealtimeServices 在消息落库后判断接收方是否在线（通过 Presence 或 Gateway 目录），
/// 离线时构造此命令调用 <see cref="IPushDispatcher.DispatchAsync"/> 触发离线推送。
/// </para>
/// <para>
/// 平台无关：投递方根据目标用户的已注册令牌选择 Provider（FCM/APNs/WebPush）。
/// </para>
/// </summary>
public sealed class PushDeliveryCommand
{
    /// <summary>目标用户 Id。</summary>
    public required long TargetUserId { get; init; }

    /// <summary>
    /// 推送标题（已本地化）。平台可能截断（APNs alert.title 上限等）。
    /// </summary>
    public required string Title { get; init; }

    /// <summary>
    /// 推送正文（已本地化）。平台可能截断。
    /// </summary>
    public required string Body { get; init; }

    /// <summary>
    /// 会话 Id，用于 Collapse Key（同一会话的推送折叠，避免锁屏刷屏）。
    /// 可空表示不折叠。
    /// </summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 消息 Id，用于去重与点击跳转。
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// 发送者显示名（用于推送文案拼接，如 "Alice: 你好"）。
    /// </summary>
    public string? SenderDisplayName { get; init; }

    /// <summary>
    /// 是否 @mention。Mention 推送优先级更高，不受静音影响（除非用户明确拒绝 mention）。
    /// </summary>
    public bool IsMention { get; init; }

    /// <summary>
    /// 自定义数据（点击跳转 payload、badge 计数等）。
    /// 平台限制：FCM data key/value 均 string；APNs 自定义 payload 1KB 内。
    /// </summary>
    public IReadOnlyDictionary<string, string>? CustomData { get; init; }

    /// <summary>消息发生时间（Unix ms），用于排序与过期判断。</summary>
    public long OccurredAtMs { get; init; }
}

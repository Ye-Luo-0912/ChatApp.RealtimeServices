namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 更新会话成员置顶 / 免打扰偏好。字段为 null 表示不修改。
/// </summary>
public sealed class ConversationSetPrefsCommand
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public required string ConversationId { get; init; }

    /// <summary>true 置顶；false 取消置顶；null 不变。</summary>
    public bool? Pinned { get; init; }

    /// <summary>true 免打扰；false 取消；null 不变。</summary>
    public bool? Muted { get; init; }

    /// <summary>
    /// 免打扰截止（Unix ms）。仅在 <see cref="Muted"/>=true 时生效；null 表示永久。
    /// </summary>
    public long? MutedUntilMs { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 会话摘要变更事件载荷（线协议类型为 <see cref="Events.RealtimeEventType.ConversationListChanged"/>）。
/// Gateway 只依赖本 DTO，不依赖 Realtime 数据库模型。
/// </summary>
public sealed class RealtimeConversationChangedPayload
{
    public const int CurrentPayloadVersion = 3;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    public required string ConversationId { get; init; }

    public ConversationType Type { get; init; } = ConversationType.Direct;

    /// <summary>
    /// 对目标用户而言的单聊对端；群聊阶段可为空。
    /// </summary>
    public long? PeerUserId { get; init; }

    /// <summary>群标题；单聊为空。</summary>
    public string? Title { get; init; }

    public string? LastMessageId { get; init; }

    public string? LastMessagePreview { get; init; }

    public long? LastMessageAtMs { get; init; }

    public long? LastSenderUserId { get; init; }

    /// <summary>v2 起可空附加：成员置顶偏好。消息投影事件可不填。</summary>
    public bool? IsPinned { get; init; }

    /// <summary>v2 起可空附加：成员免打扰偏好。消息投影事件可不填。</summary>
    public bool? IsMuted { get; init; }

    /// <summary>免打扰截止时间（Unix ms）；null 且 IsMuted=true 表示永久。</summary>
    public long? MutedUntilMs { get; init; }

    /// <summary>
    /// v3 起可空附加：会话当前最大序列号。客户端可基于本地 last_read_sequence 派生未读增量，
    /// 避免服务端在群消息写入时为每个成员生成 UnreadCountChanged 事件（O(N)→O(1) Outbox）。
    /// </summary>
    public long? LastSequence { get; init; }
}

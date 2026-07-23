namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 未读数变更事件载荷（线协议类型 <see cref="Events.RealtimeEventType.UnreadCountChanged"/>）。
/// </summary>
public sealed class RealtimeUnreadCountChangedPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }
    public int UnreadCount { get; init; }
    public string? LastReadMessageId { get; init; }
    public long? LastReadAtMs { get; init; }
}

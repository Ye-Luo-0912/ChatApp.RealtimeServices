namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 成员会话已读水位推进事件载荷（线协议类型
/// <see cref="Events.RealtimeEventType.ConversationRead"/>）。
/// 通知会话其他成员：某读者将游标推进到指定消息；每条 MarkRead 对每个目标用户一条 Outbox，避免 N²。
/// </summary>
public sealed class RealtimeConversationReadPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }
    public required long ReaderUserId { get; init; }
    public required string LastReadMessageId { get; init; }
    public long LastReadAtMs { get; init; }

    /// <summary>
    /// 已读序列水位。客户端应将此值保存为本地 last_read_sequence。
    /// </summary>
    public long? LastReadSequence { get; init; }
}

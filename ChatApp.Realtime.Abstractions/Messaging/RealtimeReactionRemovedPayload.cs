namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeReactionRemovedPayload
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long ReactorUserId { get; init; }
    public long MessageSenderUserId { get; init; }
    public long MessageReceiverUserId { get; init; }
    public required string Emoji { get; init; }
    public int EmojiCount { get; init; }
    public long OccurredAtMs { get; init; }

    /// <summary>反应目标消息在会话内的序列号（反应不推进序列）。</summary>
    public long? ConversationSequence { get; init; }
}

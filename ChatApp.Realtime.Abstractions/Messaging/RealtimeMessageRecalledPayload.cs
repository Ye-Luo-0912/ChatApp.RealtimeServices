namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeMessageRecalledPayload
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public long RecalledAtMs { get; init; }

    /// <summary>
    /// 撤回发生时会话的当前序列号（撤回不推进序列，此值为撤回时的 last_sequence 快照）。
    /// </summary>
    public long? ConversationSequence { get; init; }
}

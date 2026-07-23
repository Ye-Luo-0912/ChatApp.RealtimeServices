namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 消息送达/已读回执载荷。业务名 MessageDelivered / MessageRead 均走
/// <see cref="Events.RealtimeEventType.MessageReceiptUpdated"/>，由 <see cref="ReceiptType"/> 区分。
/// </summary>
public sealed class RealtimeMessageReceiptPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    public required string MessageId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required MessageReceiptType ReceiptType { get; init; }
    public long OccurredAtMs { get; init; }

    /// <summary>可选会话编号；旧事件可缺省。</summary>
    public string? ConversationId { get; init; }
}

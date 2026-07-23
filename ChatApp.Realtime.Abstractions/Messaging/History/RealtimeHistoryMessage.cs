namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class RealtimeHistoryMessage
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public string? ConversationId { get; init; }
    public required string Content { get; init; }
    public long ReceivedAtMs { get; init; }
    public long? DeliveredAtMs { get; init; }
    public long? ReadAtMs { get; init; }

    /// <summary>绑定附件引用；无附件时为 null 或空列表。</summary>
    public IReadOnlyList<AttachmentRef>? Attachments { get; init; }

    public string? ReplyToMessageId { get; init; }
    public long? ReplyToSenderUserId { get; init; }
    public string? ReplyToPreview { get; init; }

    public string? ForwardedFromMessageId { get; init; }
    public long? ForwardedFromSenderUserId { get; init; }
    public string? ForwardedFromPreview { get; init; }

    /// <summary>非空表示已撤回。</summary>
    public long? RecalledAtMs { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class RealtimeMessageRecord
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }

    /// <summary>
    /// 稳定会话编号。单聊由服务端按双方用户派生；写入路径必填。
    /// </summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 待绑定的已确认附件 id（与 <see cref="Messaging.IncomingMessageCommand.AttachmentIds"/> 对应）。
    /// </summary>
    public IReadOnlyList<string>? AttachmentIds { get; init; }

    public string? ReplyToMessageId { get; init; }
    public long? ReplyToSenderUserId { get; init; }
    public string? ReplyToPreview { get; init; }

    public string? ForwardedFromMessageId { get; init; }
    public long? ForwardedFromSenderUserId { get; init; }
    public string? ForwardedFromPreview { get; init; }

    public long ReceivedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

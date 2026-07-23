namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed record IncomingMessageCommand
{
    public required string CommandId { get; init; }
    public required string ClientMessageId { get; init; }

    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }

    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }

    /// <summary>
    /// 已确认附件 id 列表；写入消息时绑定到本条消息（同事务）。
    /// </summary>
    public IReadOnlyList<string>? AttachmentIds { get; init; }

    /// <summary>被回复消息的服务端 MessageId。</summary>
    public string? ReplyToMessageId { get; init; }

    /// <summary>被回复消息的发送方用户 Id（展示用）。</summary>
    public long? ReplyToSenderUserId { get; init; }

    /// <summary>被回复内容预览（客户端截断后上行，最长 256）。</summary>
    public string? ReplyToPreview { get; init; }

    /// <summary>被转发原消息的服务端 MessageId（展示用，不校验存在性）。</summary>
    public string? ForwardedFromMessageId { get; init; }

    public long? ForwardedFromSenderUserId { get; init; }

    /// <summary>被转发内容预览，最长 256。</summary>
    public string? ForwardedFromPreview { get; init; }

    public long ReceivedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

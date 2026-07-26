namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed record IncomingMessageCommand
{
    public required string CommandId { get; init; }
    public required string ClientMessageId { get; init; }

    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }

    /// <summary>
    /// 单聊对端用户。群聊可为 0（广播会话，目标由 ConversationId + 成员表决定）。
    /// </summary>
    public required long ReceiverUserId { get; init; }

    /// <summary>
    /// 显式会话 Id。群聊必填（grp:…）；单聊可空（服务端按双方派生）。
    /// </summary>
    public string? ConversationId { get; init; }

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

    /// <summary>@提到的用户 Id 列表（群聊场景下使用）。</summary>
    public IReadOnlyList<long>? MentionedUserIds { get; init; }

    /// <summary>@提到的角色（如 "all"、"admin"）；目前仅供展示，无强校验。</summary>
    public IReadOnlyList<string>? MentionedRoles { get; init; }

    public long ReceivedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

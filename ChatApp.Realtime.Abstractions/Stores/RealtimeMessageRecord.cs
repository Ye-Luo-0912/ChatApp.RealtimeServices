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

    /// <summary>@提到的用户 Id 列表（群聊场景下使用）。</summary>
    public IReadOnlyList<long>? MentionedUserIds { get; init; }

    /// <summary>@提到的角色（如 "all"、"admin"）；目前仅供展示，无强校验。</summary>
    public IReadOnlyList<string>? MentionedRoles { get; init; }

    /// <summary>
    /// P0-10：由 Processor 在 mentions sanitization 之前基于原始请求计算的内容指纹。
    /// 若非 null，SaveAsync 直接使用此值；否则 SaveAsync 内部回退到基于 sanitized mentions 重算。
    /// </summary>
    public string? RequestFingerprint { get; init; }

    public long ReceivedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
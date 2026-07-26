namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeChatMessagePayload
{
    /// <summary>v5：新增 MentionedUserIds / MentionedRoles 字段。</summary>
    public const int CurrentPayloadVersion = 5;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }

    /// <summary>
    /// 稳定会话编号；旧事件可缺省，新写入必带。
    /// </summary>
    public string? ConversationId { get; init; }

    public long ReceivedAtMs { get; init; }

    /// <summary>绑定附件；v1 事件缺省。客户端经 DownloadApiHint 拉取，非公网 URL。</summary>
    public IReadOnlyList<AttachmentRef>? Attachments { get; init; }

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
}
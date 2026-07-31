using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeChatMessagePayload
{
    /// <summary>v6：新增 ConversationType，使 MessageReceived 单事件可同时驱动会话列表更新（极限-1）。</summary>
    public const int CurrentPayloadVersion = 6;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }

    /// <summary>稳定会话编号；旧事件可缺省，新写入必带。</summary>
    public string? ConversationId { get; init; }

    /// <summary>
    /// 服务端分配的会话内单调递增序列号。客户端据此重排、检测缺口、保存 last_read_sequence。
    /// </summary>
    public long? ConversationSequence { get; init; }

    /// <summary>
    /// 极限-1：会话类型。携带后客户端从单条 MessageReceived 事件即可更新会话列表
    /// （lastMessageId / preview / lastSenderUserId / lastSequence 均可由本 payload 字段派生），
    /// 不再需要额外的 ConversationListChanged 行。旧事件缺省为 null，客户端按旧路径处理。
    /// </summary>
    public ConversationType? ConversationType { get; init; }

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

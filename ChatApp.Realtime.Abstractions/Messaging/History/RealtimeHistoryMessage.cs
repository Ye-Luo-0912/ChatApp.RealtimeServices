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

    /// <summary>内容版本，从 1 起；每次成功编辑 +1。</summary>
    public int EditVersion { get; init; } = 1;

    /// <summary>最近一次成功编辑时间；未编辑为 null。</summary>
    public long? EditedAtMs { get; init; }

    /// <summary>
    /// 变更水位：插入/编辑/撤回/反应变更时间。同步 catch-up 按此字段推进。
    /// </summary>
    public long ChangedAtMs { get; init; }

    /// <summary>反应摘要（emoji → count + ReactedByMe）；无反应时为 null 或空。</summary>
    public IReadOnlyList<MessageReactionSummary>? Reactions { get; init; }

    /// <summary>是否已编辑过（EditVersion&gt;1 或 EditedAtMs 有值）。</summary>
    public bool IsEdited => EditVersion > 1 || EditedAtMs is > 0;
}

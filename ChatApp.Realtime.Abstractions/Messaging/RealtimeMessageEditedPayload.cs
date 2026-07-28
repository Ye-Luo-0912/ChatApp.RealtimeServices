namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class RealtimeMessageEditedPayload
{
    public required string MessageId { get; init; }
    public string? ConversationId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public required string Content { get; init; }
    public int EditVersion { get; init; }
    public long EditedAtMs { get; init; }

    /// <summary>
    /// 编辑后替换的 @提及用户 Id 列表。
    /// <para>
    /// 客户端可对比 <see cref="RealtimeChatMessagePayload.MentionedUserIds"/> 旧值与本字段新值，
    /// 派生 MentionRemoved / MentionAdded 集合，无需服务端单独发 MentionRemoved 事件。
    /// </para>
    /// <para>
    /// <c>null</c> 表示本次编辑未修改 mentions，客户端应沿用上次已知值。
    /// </para>
    /// </summary>
    public IReadOnlyList<long>? MentionedUserIds { get; init; }

    /// <summary>编辑后替换的 @提及角色列表。语义同 <see cref="MentionedUserIds"/>。</summary>
    public IReadOnlyList<string>? MentionedRoles { get; init; }
}

namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 群成员批量加入事件载荷（业务名 MembersAdded）。
/// <para>
/// 用于建群 / 批量加人场景，替代逐成员 MemberJoined 的 O(N²) 扇出：
/// 一次成员变更只产生一个聚合事件，<see cref="RealtimeEvent.TargetUserIds"/> 携带全部目标成员。
/// </para>
/// </summary>
public sealed class RealtimeMembersAddedPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }

    /// <summary>本次新增的成员集合（不含创建者自身，建群时为除 Owner 外的初始成员）。</summary>
    public required IReadOnlyList<ConversationMemberItem> Members { get; init; }

    public long ActorUserId { get; init; }
    public string? Title { get; init; }
    public long OccurredAtMs { get; init; }
}

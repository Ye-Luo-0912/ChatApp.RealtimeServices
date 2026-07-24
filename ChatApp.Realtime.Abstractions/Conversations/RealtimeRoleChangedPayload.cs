namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>成员角色变更事件载荷（含所有权转让）。</summary>
public sealed class RealtimeRoleChangedPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public ConversationMemberRole NewRole { get; init; }
    public ConversationMemberRole? PreviousRole { get; init; }
    public long ActorUserId { get; init; }
    public long OccurredAtMs { get; init; }
}

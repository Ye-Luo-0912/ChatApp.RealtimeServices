namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>群成员加入事件载荷。</summary>
public sealed class RealtimeMemberJoinedPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public ConversationMemberRole Role { get; init; } = ConversationMemberRole.Member;
    public long ActorUserId { get; init; }
    public string? Title { get; init; }
    public long OccurredAtMs { get; init; }
}

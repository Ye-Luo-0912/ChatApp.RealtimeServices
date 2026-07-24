namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationMemberItem
{
    public required long UserId { get; init; }
    public ConversationMemberRole Role { get; init; } = ConversationMemberRole.Member;
    public long JoinedAtMs { get; init; }
}

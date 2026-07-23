namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationListQuery
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public bool? BeforeIsPinned { get; init; }
    public long? BeforePinnedAtMs { get; init; }
    public long? BeforeLastMessageAtMs { get; init; }
    public string? BeforeConversationId { get; init; }
    public int Limit { get; init; } = 50;
}

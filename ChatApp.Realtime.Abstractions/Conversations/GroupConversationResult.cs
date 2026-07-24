namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class GroupConversationResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ConversationId { get; init; }
    public string? Title { get; init; }
    public ConversationType Type { get; init; } = ConversationType.Group;
    public IReadOnlyList<ConversationMemberItem>? Members { get; init; }

    public static GroupConversationResult Success(
        string requestId,
        string conversationId,
        string? title = null,
        IReadOnlyList<ConversationMemberItem>? members = null) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            ConversationId = conversationId,
            Title = title,
            Members = members
        };

    public static GroupConversationResult Failed(
        string requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
}

namespace ChatApp.Realtime.Abstractions.Messaging.History;

/// <summary>历史/同步用的反应摘要：emoji → 计数，以及当前用户是否已点。</summary>
public sealed class MessageReactionSummary
{
    public required string Emoji { get; init; }
    public int Count { get; init; }
    public bool ReactedByMe { get; init; }
}

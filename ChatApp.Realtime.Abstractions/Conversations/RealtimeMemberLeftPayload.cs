namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>成员主动退群事件载荷。</summary>
public sealed class RealtimeMemberLeftPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;
    public required string ConversationId { get; init; }
    public required long UserId { get; init; }
    public long OccurredAtMs { get; init; }
}

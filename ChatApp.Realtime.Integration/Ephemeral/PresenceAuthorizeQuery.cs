namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>Gateway → Server：批量校验 Presence 查询目标（好友或会话成员）。</summary>
public sealed class PresenceAuthorizeQuery
{
    public long WatcherUserId { get; init; }
    public IReadOnlyList<long> TargetUserIds { get; init; } = [];
}

namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>Server → Gateway：允许查询/订阅的用户 Id 列表。</summary>
public sealed class PresenceAuthorizeResponse
{
    public IReadOnlyList<long> AllowedUserIds { get; init; } = [];
}

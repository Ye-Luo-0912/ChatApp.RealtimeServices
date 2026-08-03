using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Integration.Ephemeral;

/// <summary>
/// 三-3：Gateway → Server：查询单个用户的生命周期状态（用于 FrozenUserCache 后台刷新）。
/// </summary>
public sealed class UserLifecycleQuery
{
    public long UserId { get; init; }
}

/// <summary>
/// 三-3：Server → Gateway：用户生命周期查询响应。
/// </summary>
public sealed class UserLifecycleResponse
{
    /// <summary>用户当前生命周期状态。</summary>
    public UserLifecycleState State { get; init; } = UserLifecycleState.Active;
}

using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 三-3：用户生命周期变更事件 Payload。
/// <para>
/// Server 冻结/解冻用户时发布 <see cref="RealtimeEventType.UserLifecycleChanged"/> 事件，
/// Payload 携带新状态与变更时间。Gateway 收到后更新 FrozenUserCache 并关闭冻结用户的活跃会话。
/// </para>
/// </summary>
public sealed class RealtimeUserLifecycleChangedPayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    /// <summary>变更后的生命周期状态。</summary>
    public required UserLifecycleState NewState { get; init; }

    /// <summary>变更发生的 Unix 毫秒时间戳。</summary>
    public long ChangedAtMs { get; init; }

    /// <summary>变更原因（管理员操作备注，可选）。</summary>
    public string? Reason { get; init; }
}

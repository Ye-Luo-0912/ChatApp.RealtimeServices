namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 用户生命周期状态。
/// <para>
/// Active：正常状态（无 tombstone 行）。</para>
/// <para>
/// Deleting：账号删除清理进行中。所有写入操作必须拒绝。</para>
/// <para>
/// Deleted：账号删除清理已完成。旧命令回放必须拒绝，直到 tombstone 过期。</para>
/// </summary>
public enum UserLifecycleState : byte
{
    /// <summary>无 tombstone 行 — 用户处于正常活跃状态。</summary>
    Active = 0,

    /// <summary>账号删除清理进行中 — 拒绝所有新写入操作。</summary>
    Deleting = 1,

    /// <summary>账号删除清理已完成 — 拒绝旧命令回放。</summary>
    Deleted = 2
}

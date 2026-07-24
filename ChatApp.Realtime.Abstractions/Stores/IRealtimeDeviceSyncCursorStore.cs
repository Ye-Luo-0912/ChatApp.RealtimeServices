using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 设备级同步游标：与用户级已读分离，仅用于重连 catch-up。
/// </summary>
public interface IRealtimeDeviceSyncCursorStore
{
    Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
        long userId,
        ulong deviceIdHash,
        int take,
        CancellationToken ct = default);

    Task UpsertManyAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<DeviceSyncCursor> cursors,
        CancellationToken ct = default);

    /// <summary>
    /// 删除指定会话的设备游标（例如触发 ResetRequired 后清除，避免下次 bootstrap 复用已失效游标）。
    /// </summary>
    Task DeleteAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<string> conversationIds,
        CancellationToken ct = default);

    /// <summary>账号删除时清理该用户全部设备游标。</summary>
    Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default);
}

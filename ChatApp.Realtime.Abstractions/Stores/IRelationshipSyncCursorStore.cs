using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 关系列表设备级同步游标存储。
/// <para>
/// 与 <see cref="IRealtimeDeviceSyncCursorStore"/> 平行，但以 list_type 为维度
/// 而非 conversation_id。用于 SyncBootstrap 的 Relationship 增量同步。
/// </para>
/// </summary>
public interface IRelationshipSyncCursorStore
{
    /// <summary>
    /// 加载指定用户+设备的所有关系列表游标。
    /// </summary>
    Task<IReadOnlyList<RelationshipSyncCursor>> LoadAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken ct = default);

    /// <summary>
    /// 批量 upsert 游标（单调推进：仅当新水位 > 已存水位时更新）。
    /// </summary>
    Task UpsertManyAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<RelationshipSyncCursor> cursors,
        CancellationToken ct = default);

    /// <summary>删除指定列表类型的游标（触发 reset 后清除）。</summary>
    Task DeleteAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<byte> listTypes,
        CancellationToken ct = default);

    /// <summary>账号删除时清理。</summary>
    Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default);

    /// <summary>清理长期未活跃游标。</summary>
    Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default);
}

/// <summary>关系列表同步游标持久化记录。</summary>
public sealed class RelationshipSyncCursor
{
    public required byte ListType { get; init; }
    public long AfterChangedAtMs { get; init; }
    public long UpdatedAtMs { get; init; }
    public long LastSeenAtMs { get; init; }
}
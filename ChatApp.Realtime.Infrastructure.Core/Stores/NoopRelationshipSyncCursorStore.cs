using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 占位实现：关系同步游标存储未配置时返回空，所有写操作为 no-op。
/// </summary>
public sealed class NoopRelationshipSyncCursorStore : IRelationshipSyncCursorStore
{
    public Task<IReadOnlyList<RelationshipSyncCursor>> LoadAsync(
        long userId, ulong deviceIdHash, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RelationshipSyncCursor>>(Array.Empty<RelationshipSyncCursor>());

    public Task UpsertManyAsync(
        long userId, ulong deviceIdHash,
        IReadOnlyList<RelationshipSyncCursor> cursors, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task DeleteAsync(
        long userId, ulong deviceIdHash,
        IReadOnlyList<byte> listTypes, CancellationToken ct = default) =>
        Task.CompletedTask;

    public Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default) =>
        Task.FromResult(0L);

    public Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default) =>
        Task.FromResult(0L);
}
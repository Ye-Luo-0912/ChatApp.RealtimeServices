using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class UnavailableRelationshipProjectionQueryStore
    : IRelationshipProjectionQueryStore
{
    public static UnavailableRelationshipProjectionQueryStore Instance { get; } = new();

    private UnavailableRelationshipProjectionQueryStore()
    {
    }

    public Task<RelationshipProjectionReadPage> ReadAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        int pageSize,
        long? expectedVersion,
        string? afterResourceId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(new RelationshipProjectionReadPage(
            RelationshipProjectionReadStatus.Unavailable,
            0,
            [],
            false,
            null));
    }
}

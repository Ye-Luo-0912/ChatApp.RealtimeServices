using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Stores;

public enum RelationshipProjectionReadStatus : byte
{
    Ready = 1,
    Unavailable = 2,
    VersionChanged = 3,
}

public sealed record RelationshipProjectionReadPage(
    RelationshipProjectionReadStatus Status,
    long CurrentVersion,
    IReadOnlyList<RelationshipListItem> Items,
    bool HasMore,
    string? NextResourceId);

/// <summary>
/// Reads only Server-authoritative relationship projection streams that have a durable
/// snapshot checkpoint. Implementations must keep the version and page in one consistent
/// database snapshot and reject a continuation cursor after the stream version changes.
/// </summary>
public interface IRelationshipProjectionQueryStore
{
    Task<RelationshipProjectionReadPage> ReadAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        int pageSize,
        long? expectedVersion,
        string? afterResourceId,
        CancellationToken ct = default);
}

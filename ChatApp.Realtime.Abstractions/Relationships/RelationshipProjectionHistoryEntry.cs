namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// One applied, versioned change in an owner's relationship projection history. Rows are
/// appended in the same transaction as the projection item, inbox and stream version
/// advance, and are uniquely keyed by (owner, list, version). Catch-up reads replay these
/// entries strictly after a client's confirmed version.
/// </summary>
public sealed class RelationshipProjectionHistoryEntry
{
    public required long OwnerUserId { get; init; }
    public required RelationshipProjectionListType ListType { get; init; }
    public required long Version { get; init; }
    public required string EventId { get; init; }
    public required RelationshipProjectionOperation Operation { get; init; }
    public required string ResourceId { get; init; }
    public required long SubjectUserId { get; init; }
    public required long ActorUserId { get; init; }
    public string? State { get; init; }
    public string? Message { get; init; }
    public required long OccurredAtMs { get; init; }
}
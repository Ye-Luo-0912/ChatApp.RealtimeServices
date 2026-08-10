namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// Server-authoritative relationship list kinds. Numeric values are stable wire values.
/// </summary>
public enum RelationshipProjectionListType : byte
{
    FriendRequests = 1,
    Friends = 2,
    BlockedUsers = 3,
}

/// <summary>
/// Operation applied to one item in an owner's relationship projection.
/// </summary>
public enum RelationshipProjectionOperation : byte
{
    Upsert = 1,
    Delete = 2,
}

/// <summary>
/// Versioned delta emitted by ChatApp.Server in the same transaction as the authoritative write.
/// Version is contiguous per (OwnerUserId, ListType); consumers must reject gaps.
/// </summary>
public sealed class RelationshipProjectionDelta
{
    public const int CurrentSchemaVersion = 1;

    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
    public required string EventId { get; init; }
    public required long OwnerUserId { get; init; }
    public required RelationshipProjectionListType ListType { get; init; }
    public required long Version { get; init; }
    public required RelationshipProjectionOperation Operation { get; init; }
    public required string ResourceId { get; init; }
    public required long SubjectUserId { get; init; }
    public required long ActorUserId { get; init; }
    public string? State { get; init; }
    public string? Message { get; init; }
    public required long OccurredAtMs { get; init; }
}

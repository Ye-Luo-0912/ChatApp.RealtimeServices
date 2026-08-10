namespace ChatApp.Realtime.Abstractions.Events;

using ChatApp.Realtime.Abstractions.Relationships;

public sealed class RealtimeDomainNotificationPayload
{
    public required string Resource { get; init; }
    public required string Action { get; init; }
    public string? ResourceId { get; init; }
    public string? Message { get; init; }

    /// <summary>
    /// Optional Server-authoritative relationship projection delta. Older consumers ignore it
    /// and continue treating this payload as an online list invalidation only.
    /// </summary>
    public RelationshipProjectionDelta? Projection { get; init; }
}

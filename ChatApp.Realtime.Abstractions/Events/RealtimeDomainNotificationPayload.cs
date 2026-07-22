namespace ChatApp.Realtime.Abstractions.Events;

public sealed class RealtimeDomainNotificationPayload
{
    public required string Resource { get; init; }
    public required string Action { get; init; }
    public string? ResourceId { get; init; }
    public string? Message { get; init; }
}

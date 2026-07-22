namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class DeadLetterMessage
{
    public required string DeadLetterId { get; init; }
    public string? CommandId { get; init; }
    public required string SourceSubject { get; init; }
    public required string ReasonCode { get; init; }
    public required string Reason { get; init; }
    public string? Payload { get; init; }
    public ulong? DeliveryCount { get; init; }
    public long FailedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

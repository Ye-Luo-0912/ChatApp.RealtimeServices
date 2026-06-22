namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

public sealed class NatsOptions
{
    public string? Url { get; init; }
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
}

public sealed class NatsSubjectOptions
{
    public required string IncomingMessages { get; init; }
    public required string RealtimeEvents { get; init; }
    public string? MessagePersistence { get; init; }
}

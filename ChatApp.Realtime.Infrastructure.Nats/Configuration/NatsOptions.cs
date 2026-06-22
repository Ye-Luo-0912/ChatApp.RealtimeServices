namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

public sealed class NatsOptions
{
    public string? Url { get; init; }
    public string Mode { get; init; } = "Core";
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
    public JetStreamOptions? JetStream { get; init; }
}

public sealed class NatsSubjectOptions
{
    public required string IncomingMessages { get; init; }
    public required string RealtimeEvents { get; init; }
    public string? MessagePersistence { get; init; }
}

public sealed class JetStreamOptions
{
    public JetStreamStreamOptions Streams { get; init; } = new();
}

public sealed class JetStreamStreamOptions
{
    public string IncomingMessages { get; init; } = "INCOMING_MESSAGES";
    public string RealtimeEvents { get; init; } = "REALTIME_EVENTS";
}

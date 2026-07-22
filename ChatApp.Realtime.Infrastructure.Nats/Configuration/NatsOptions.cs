namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

public sealed class NatsOptions
{
    public string? Url { get; init; }
    public string Mode { get; init; } = "JetStream";
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
    public JetStreamOptions? JetStream { get; init; }
}

public sealed class NatsSubjectOptions
{
    public required string IncomingMessages { get; init; }
    public string MessageReceipts { get; init; } = "chat.message-receipts";
    public required string RealtimeEvents { get; init; }
    public string AccountCleanup { get; init; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueries { get; init; } = "chat.message-history.query";
    public string? MessagePersistence { get; init; }
    public string DeadLetters { get; init; } = "chat.dead-letters";
}

public sealed class JetStreamOptions
{
    public JetStreamStreamOptions Streams { get; init; } = new();
    public JetStreamConsumerOptions Consumer { get; init; } = new();
    public int Replicas { get; init; } = 1;
    public long MaxBytes { get; init; } = 10L * 1024 * 1024 * 1024;
    public int MaxMessageSize { get; init; } = 1024 * 1024;
    public int MaxAgeHours { get; init; } = 168;
    public int DeadLetterMaxAgeHours { get; init; } = 720;
    public int DuplicateWindowMinutes { get; init; } = 10;
}

public sealed class JetStreamStreamOptions
{
    public string IncomingMessages { get; init; } = "INCOMING_MESSAGES";
    public string MessageReceipts { get; init; } = "MESSAGE_RECEIPTS";
    public string RealtimeEvents { get; init; } = "REALTIME_EVENTS";
    public string DeadLetters { get; init; } = "DEAD_LETTERS";
}

public sealed class JetStreamConsumerOptions
{
    public int AckWaitSeconds { get; init; } = 60;
    public int MaxDeliver { get; init; } = 10;
    public int MaxAckPending { get; init; } = 256;
    public int[] BackoffSeconds { get; init; } = [1, 5, 30, 120, 300];
}

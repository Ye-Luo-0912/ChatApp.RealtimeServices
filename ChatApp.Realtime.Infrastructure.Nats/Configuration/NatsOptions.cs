namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

public sealed class NatsOptions
{
    public string? Url { get; init; }
    public string Mode { get; init; } = "JetStream";
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
    public JetStreamOptions? JetStream { get; init; }
    public NatsAuthOptions Auth { get; init; } = new();
    public NatsTrustOptions Trust { get; init; } = new();
}

public sealed class NatsAuthOptions
{
    public string? Username { get; init; }
    public string? Password { get; init; }
    public string? Token { get; init; }
    public string? CredsFile { get; init; }
    public string? NKey { get; init; }
    public string? Seed { get; init; }
    public string? NKeyFile { get; init; }
}

/// <summary>
/// 入站/历史查询对网关身份头的校验策略。Production 默认要求网关身份头并拒绝伪造 sender。
/// </summary>
public sealed class NatsTrustOptions
{
    /// <summary>
    /// null = 按环境默认（非 Development 为 true）；显式 true/false 覆盖。
    /// </summary>
    public bool? RequireGatewayIdentity { get; init; }

    public string UserIdHeader { get; init; } = ChatApp.Realtime.Abstractions.Auth.RealtimeIdentityHeaders.UserId;
    public string SessionIdHeader { get; init; } = ChatApp.Realtime.Abstractions.Auth.RealtimeIdentityHeaders.SessionId;
}

public sealed class NatsSubjectOptions
{
    public required string IncomingMessages { get; init; }
    public string MessageReceipts { get; init; } = "chat.message-receipts";
    public required string RealtimeEvents { get; init; }
    public string AccountCleanup { get; init; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueries { get; init; } = "chat.message-history.query";
    public string ConversationListQueries { get; init; } = "chat.conversation-list.query";
    public string ConversationMarkReads { get; init; } = "chat.conversation-mark-read";
    public string ConversationSetPrefs { get; init; } = "chat.conversation-prefs.set";
    public string MessageRecalls { get; init; } = "chat.message-recall";
    public string SyncBootstrapQueries { get; init; } = "chat.sync.bootstrap";
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

namespace ChatApp.Realtime.Integration.Configuration;

public sealed class RealtimeIntegrationOptions
{
    public string Url { get; init; } = "nats://127.0.0.1:4222";
    public string ClientName { get; init; } = "chatapp-realtime-client";
    public string InstanceId { get; init; } = Environment.MachineName;
    public string IncomingMessagesSubject { get; init; } = "chat.incoming-messages";
    public string MessageReceiptsSubject { get; init; } = "chat.message-receipts";
    public string RealtimeEventsSubject { get; init; } = "chat.realtime-events";
    /// <summary>账号清理 subject（UserAccountDeleted / AccountCleanupCompleted）。</summary>
    public string AccountCleanupSubject { get; init; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueriesSubject { get; init; } = "chat.message-history.query";
    public string DeadLettersSubject { get; init; } = "chat.dead-letters";
    public string IncomingMessagesStream { get; init; } = "INCOMING_MESSAGES";
    public string MessageReceiptsStream { get; init; } = "MESSAGE_RECEIPTS";
    public string RealtimeEventsStream { get; init; } = "REALTIME_EVENTS";
    public string DeadLettersStream { get; init; } = "DEAD_LETTERS";
    public string GatewayConsumerPrefix { get; init; } = "chatapp-tcp-gateway";
    /// <summary>
    /// Server Saga 等共享 durable（非网关每实例独立 consumer）。
    /// 留空则回退为 GatewayConsumerPrefix + InstanceId。
    /// </summary>
    public string AccountCleanupConsumerName { get; init; } = "chatapp-server-account-cleanup-saga";
    public bool ManageStreams { get; init; }
    /// <summary>
    /// 新建 consumer 时是否从流头回放（默认仅投递新建后的消息）。
    /// </summary>
    public bool ReplayRetainedEventsOnConsumerCreation { get; init; }
    public int Replicas { get; init; } = 1;
    public long MaxBytes { get; init; } = 10L * 1024 * 1024 * 1024;
    public int MaxMessageSize { get; init; } = 1024 * 1024;
    public int MaxAgeHours { get; init; } = 168;
    public int DeadLetterMaxAgeHours { get; init; } = 720;
    public int DuplicateWindowMinutes { get; init; } = 10;
    public int AckWaitSeconds { get; init; } = 60;
    public int MaxDeliver { get; init; } = 10;
    public int MaxAckPending { get; init; } = 512;
    public int HistoryRequestTimeoutMs { get; init; } = 3_000;
    public int[] BackoffSeconds { get; init; } = [1, 5, 30, 120, 300];
}

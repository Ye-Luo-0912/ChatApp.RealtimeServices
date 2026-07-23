namespace ChatApp.Realtime.Integration.Configuration;

public sealed class RealtimeIntegrationOptions
{
    public string Url { get; set; } = "nats://127.0.0.1:4222";
    public string ClientName { get; set; } = "chatapp-realtime-client";
    public string InstanceId { get; set; } = Environment.MachineName;
    public string IncomingMessagesSubject { get; set; } = "chat.incoming-messages";
    public string MessageReceiptsSubject { get; set; } = "chat.message-receipts";
    public string RealtimeEventsSubject { get; set; } = "chat.realtime-events";
    /// <summary>账号清理 subject（UserAccountDeleted / AccountCleanupCompleted）。</summary>
    public string AccountCleanupSubject { get; set; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueriesSubject { get; set; } = "chat.message-history.query";
    public string ConversationListQueriesSubject { get; set; } = "chat.conversation-list.query";
    public string ConversationMarkReadsSubject { get; set; } = "chat.conversation-mark-read";
    public string ConversationSetPrefsSubject { get; set; } = "chat.conversation-prefs.set";
    public string MessageRecallsSubject { get; set; } = "chat.message-recall";
    public string SyncBootstrapQueriesSubject { get; set; } = "chat.sync.bootstrap";
    public string DeadLettersSubject { get; set; } = "chat.dead-letters";
    public string IncomingMessagesStream { get; set; } = "INCOMING_MESSAGES";
    public string MessageReceiptsStream { get; set; } = "MESSAGE_RECEIPTS";
    public string RealtimeEventsStream { get; set; } = "REALTIME_EVENTS";
    public string DeadLettersStream { get; set; } = "DEAD_LETTERS";
    public string GatewayConsumerPrefix { get; set; } = "chatapp-tcp-gateway";
    /// <summary>
    /// Server Saga 等共享 durable（非网关每实例独立 consumer）。
    /// 留空则回退为 GatewayConsumerPrefix + InstanceId。
    /// </summary>
    public string AccountCleanupConsumerName { get; set; } = "chatapp-server-account-cleanup-saga";
    public bool ManageStreams { get; set; }
    /// <summary>
    /// 新建 consumer 时是否从流头回放（默认仅投递新建后的消息）。
    /// </summary>
    public bool ReplayRetainedEventsOnConsumerCreation { get; set; }
    public int Replicas { get; set; } = 1;
    public long MaxBytes { get; set; } = 10L * 1024 * 1024 * 1024;
    public int MaxMessageSize { get; set; } = 1024 * 1024;
    public int MaxAgeHours { get; set; } = 168;
    public int DeadLetterMaxAgeHours { get; set; } = 720;
    public int DuplicateWindowMinutes { get; set; } = 10;
    public int AckWaitSeconds { get; set; } = 60;
    public int MaxDeliver { get; set; } = 10;
    public int MaxAckPending { get; set; } = 512;
    public int HistoryRequestTimeoutMs { get; set; } = 3_000;
    public int[] BackoffSeconds { get; set; } = [1, 5, 30, 120, 300];

    /// <summary>NATS 客户端认证（与 RealtimeServices Nats:Auth 对齐）。</summary>
    public RealtimeIntegrationAuthOptions Auth { get; set; } = new();
}

public sealed class RealtimeIntegrationAuthOptions
{
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? Token { get; set; }
    public string? CredsFile { get; set; }
    public string? NKey { get; set; }
    public string? Seed { get; set; }
    public string? NKeyFile { get; set; }
}

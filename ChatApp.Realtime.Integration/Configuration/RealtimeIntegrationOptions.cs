using ChatApp.Realtime.Abstractions.Routing;

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
    public string MessageEditsSubject { get; set; } = "chat.message-edit";
    public string MessageReactionsSubject { get; set; } = "chat.message-reaction";
    public string SyncBootstrapQueriesSubject { get; set; } = "chat.sync.bootstrap";
    public string GroupConversationsSubject { get; set; } = "chat.group-conversation";
    public string DeadLettersSubject { get; set; } = "chat.dead-letters";

    /// <summary>推送投递命令 subject（RealtimeServices 发布，Gateway 消费后执行实际推送）。</summary>
    public string PushDeliveriesSubject { get; set; } = "chat.push-deliveries";

    /// <summary>NATS Core ephemeral Typing（非 JetStream，每实例全量订阅）。</summary>
    public string EphemeralTypingSubject { get; set; } = "chat.ephemeral.typing";

    /// <summary>NATS Core ephemeral Presence（非 JetStream，每实例全量订阅）。</summary>
    public string EphemeralPresenceSubject { get; set; } = "chat.ephemeral.presence";

    /// <summary>NATS Core request/reply：Presence 好友鉴权。</summary>
    public string PresenceAuthorizeSubject { get; set; } = "chat.presence.authorize";

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

    /// <summary>推送投递命令 JetStream 流名称。</summary>
    public string PushDeliveriesStream { get; set; } = "PUSH_DELIVERIES";

    /// <summary>
    /// 推送投递共享 durable consumer 名称（Gateway 消费）。
    /// </summary>
    public string PushConsumerName { get; set; } = "chatapp-tcp-gateway-push";

    /// <summary>推送投递流的最大保留时长（小时）。</summary>
    public int PushMaxAgeHours { get; set; } = 24;

    /// <summary>推送投递 consumer 的 ACK 等待时长（秒）。</summary>
    public int PushAckWaitSeconds { get; set; } = 30;

    /// <summary>推送投递 consumer 的最大投递次数（超过则终止）。</summary>
    public int PushMaxDeliver { get; set; } = 3;

    /// <summary>推送投递 consumer 的最大待 ACK 消息数。</summary>
    public int PushMaxAckPending { get; set; } = 256;

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

    // ---- 第三阶段：大规模路由 ----

    /// <summary>
    /// Realtime Event 投递路由模式。默认广播（向后兼容），设为 Sharded 启用按 Gateway 分片投递。
    /// </summary>
    public EventRoutingMode RoutingMode { get; set; } = EventRoutingMode.Broadcast;

    /// <summary>
    /// Realtime Event 分片 subject 模板，使用 {0} 作为实例 ID 占位符。
    /// <para>
    /// Sharded 模式下，Gateway 订阅此 subject（填入自身 InstanceId），
    /// 发布方按目标用户的在线 Gateway 集合定向投递到此 subject。
    /// </para>
    /// </summary>
    public string RealtimeEventsShardSubjectPattern { get; set; } = "chat.realtime-events.{0}";

    /// <summary>
    /// Ephemeral Typing 分片 subject 模板，使用 {0} 作为实例 ID 占位符。
    /// <para>
    /// Sharded 模式下，Gateway 订阅此 subject（填入自身 InstanceId），
    /// 发布方按目标用户的在线 Gateway 集合定向投递。
    /// </para>
    /// </summary>
    public string EphemeralTypingShardSubjectPattern { get; set; } = "chat.ephemeral.typing.{0}";

    /// <summary>
    /// Ephemeral Presence 分片 subject 模板，使用 {0} 作为实例 ID 占位符。
    /// <para>
    /// Sharded 模式下，Gateway 订阅此 subject（填入自身 InstanceId），
    /// 发布方按观察者的在线 Gateway 集合定向投递。
    /// </para>
    /// </summary>
    public string EphemeralPresenceShardSubjectPattern { get; set; } = "chat.ephemeral.presence.{0}";
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

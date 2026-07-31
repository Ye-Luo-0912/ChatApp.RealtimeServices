using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

public sealed class NatsOptions
{
    public string? Url { get; init; }
    /// <summary>
    /// NATS 传输模式。JetStream（默认）或 Core。
    /// <para>
    /// <b>JetStream</b>：durable 命令（入站消息、回执、实时事件、死信）走 JetStream 持久化流，
    /// 查询走 Core NATS request/reply。生产环境必须使用此模式。
    /// </para>
    /// <para>
    /// <b>Core</b>：仅注册查询 Consumer（Core NATS request/reply，ephemeral 语义）。
    /// durable 接口保持 Noop——Core NATS 无持久化/重试/DLQ 语义，durable 命令在此模式下会静默丢失数据。
    /// 如需 durable 命令，必须配置 JetStream。如不需要任何 NATS 命令，留空 <see cref="Url"/> 使队列回退为 Noop。
    /// </para>
    /// </summary>
    public string Mode { get; init; } = "JetStream";
    public required string QueueGroup { get; init; }
    public required NatsSubjectOptions Subjects { get; init; }
    public JetStreamOptions? JetStream { get; init; }
    public NatsAuthOptions Auth { get; init; } = new();
    public NatsTrustOptions Trust { get; init; } = new();
    /// <summary>
    /// Realtime Event 投递路由配置。默认广播；设为 Sharded 启用按 Gateway 分片投递。
    /// <para>
    /// Sharded 模式要求 <c>ConnectionStrings:Garnet</c> 已配置（注册真实 <c>IGatewayDirectory</c>）；
    /// 否则启动校验会失败（生产）或回退到广播（开发）。
    /// </para>
    /// </summary>
    public NatsRoutingOptions Routing { get; init; } = new();
}

/// <summary>
/// NATS 路由分片配置。映射到 <see cref="RealtimeQueueOptions.RealtimeEventsShardSubjectPattern"/>。
/// </summary>
public sealed class NatsRoutingOptions
{
    /// <summary>
    /// 路由模式。默认 <see cref="EventRoutingMode.Broadcast"/>（向后兼容）。
    /// </summary>
    public EventRoutingMode Mode { get; init; } = EventRoutingMode.Broadcast;

    /// <summary>
    /// Realtime Event 分片 subject 模板，使用 {0} 作为实例 ID 占位符。
    /// <para>
    /// Sharded 模式下，Gateway 订阅此 subject（填入自身 InstanceId），
    /// 发布方按目标用户的在线 Gateway 集合定向投递。
    /// 留空则使用默认 <c>chat.realtime-events.{0}</c>。
    /// </para>
    /// </summary>
    public string? RealtimeEventsShardSubjectPattern { get; init; }

    /// <summary>
    /// 分片发布有限并行度，映射到 <see cref="RealtimeQueueOptions.ShardPublishParallelism"/>。
    /// 默认 4。小于 1 时按 1 处理。
    /// </summary>
    public int ShardPublishParallelism { get; init; } = 4;
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
    public string MessageEdits { get; init; } = "chat.message-edit";
    public string MessageReactions { get; init; } = "chat.message-reaction";
    public string SyncBootstrapQueries { get; init; } = "chat.sync.bootstrap";
    public string GroupConversations { get; init; } = "chat.group-conversation";
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
    /// <summary>
    /// JetStream 消息去重窗口（分钟）。
    /// Reliability-4：必须覆盖 Outbox 最坏情况重试周期（MaxAttempts=10, MaxRetryDelay=300s
    /// 时约 815 秒）。默认 15 分钟（900 秒），留有余量。
    /// 启动时 RealtimeStartupReporter 校验此值 >= Outbox 最坏重试周期。
    /// </summary>
    public int DuplicateWindowMinutes { get; init; } = 15;
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

    /// <summary>
    /// Reliability-4：单次 consume 拉取的最大消息数（预取上限）。
    /// 默认 16，与 ProcessingConcurrency（默认 4）的 4 倍匹配，确保有少量缓冲但不堆积。
    /// 旧实现使用 MaxAckPending（默认 256）作为 MaxMsgs，导致大量消息进入本地 Channel
    /// 后在队列中等待，消耗 AckWait 预算，最终被 JetStream 重投甚至累计到毒丸阈值。
    /// MaxAckPending 仍是 consumer 的在途上限（允许重投缓冲），但单次拉取应受此值约束。
    /// </summary>
    public int PrefetchMaxMsgs { get; init; } = 16;
}

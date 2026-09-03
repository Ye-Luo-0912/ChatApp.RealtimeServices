namespace ChatApp.Realtime.Abstractions.Queueing;

/// <summary>
/// 实时服务内部使用的消息主题名称。
/// 当前 NATS 实现会把这些名称作为 subject 使用。
/// </summary>
public sealed class RealtimeQueueTopics
{
    public required string IncomingMessages { get; init; }
    public required string MessageReceipts { get; init; }
    public required string RealtimeEvents { get; init; }
    /// <summary>账号删除清理专用 subject，避免清理消费者 ACK 无关网关事件。</summary>
    public string AccountCleanup { get; init; } = "chat.realtime-events.account-deleted";
    public string MessageHistoryQueries { get; init; } = "chat.message-history.query";
    public string ConversationListQueries { get; init; } = "chat.conversation-list.query";
    public string ConversationMarkReads { get; init; } = "chat.conversation-mark-read";
    public string ConversationSetPrefs { get; init; } = "chat.conversation-prefs.set";
    /// <summary>会话成员免打扰批量查询（Gateway → Realtime，Core NATS request/reply）。</summary>
    public string ConversationMutesQueries { get; init; } = "chat.conversation-mutes.query";
    public string MessageRecalls { get; init; } = "chat.message-recall";
    public string MessageEdits { get; init; } = "chat.message-edit";
    public string MessageReactions { get; init; } = "chat.message-reaction";
    public string SyncBootstrapQueries { get; init; } = "chat.sync.bootstrap";
    public string GroupConversations { get; init; } = "chat.group-conversation";
    public string AttachmentFinalize { get; init; } = "chat.attachment-finalize";
    public string AttachmentScan { get; init; } = "chat.attachment-scan";
    public string RelationshipCommands { get; init; } = "chat.relationship.command";
    public string RelationshipListQueries { get; init; } = "chat.relationship-list.query";
    public string? MessagePersistence { get; init; }
    public string DeadLetters { get; init; } = "chat.dead-letters";

    /// <summary>
    /// 通话即时信令（SDP/ICE）Core NATS subject。零持久化：绝不进入 JetStream 流、
    /// PostgreSQL 或持久化 Outbox。仅网关在内存中订阅转发。
    /// </summary>
    public string CallSignals { get; init; } = "chat.call-signals";

    /// <summary>通话信令命令（invite/accept/…）Core NATS subject。零持久化。</summary>
    public string CallCommands { get; init; } = "chat.call-commands";
}

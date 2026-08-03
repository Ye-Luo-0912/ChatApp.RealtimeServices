using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.Realtime.Integration.Push;

namespace ChatApp.Realtime.Integration;

public interface IRealtimeMessageBus
{
    Task PublishIncomingMessageAsync(IncomingMessageCommand command, CancellationToken ct = default);
    Task PublishMessageReceiptAsync(MessageReceiptCommand command, CancellationToken ct = default);
    Task<MessageHistoryPage> QueryMessageHistoryAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default);
    Task<ConversationListPage> QueryConversationListAsync(
        ConversationListQuery query,
        CancellationToken ct = default);
    Task<ConversationMarkReadResult> MarkConversationReadAsync(
        ConversationMarkReadCommand command,
        CancellationToken ct = default);
    Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
        ConversationSetPrefsCommand command,
        CancellationToken ct = default);
    Task<GroupConversationResult> MutateGroupConversationAsync(
        GroupConversationCommand command,
        CancellationToken ct = default);
    /// <summary>附件上传确认：Ticketed → Uploaded 状态转换。</summary>
    Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(
        AttachmentFinalizeCommand command,
        CancellationToken ct = default);

    /// <summary>关系变更命令（发送/接受/拒绝好友请求、删除好友、拉黑/取消拉黑）。</summary>
    Task<RelationshipCommandResult> MutateRelationshipAsync(
        RelationshipCommand command,
        CancellationToken ct = default);

    /// <summary>关系列表查询（好友 / 好友请求 / 黑名单）。</summary>
    Task<RelationshipListResult> QueryRelationshipListAsync(
        RelationshipListQuery query,
        CancellationToken ct = default);

    Task<MessageRecallResult> RecallMessageAsync(
        MessageRecallCommand command,
        CancellationToken ct = default);
    Task<MessageEditResult> EditMessageAsync(
        MessageEditCommand command,
        CancellationToken ct = default);
    Task<MessageReactionResult> ReactToMessageAsync(
        MessageReactionCommand command,
        CancellationToken ct = default);
    Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
        SyncBootstrapQuery query,
        CancellationToken ct = default);

    /// <summary>按消息 Id 查询；UserId 须为参与方（发送或接收）。</summary>
    Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
        long userId,
        string messageId,
        CancellationToken ct = default);

    Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default);
    IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(CancellationToken ct = default);

    /// <summary>
    /// 订阅账号清理 subject（AccountCleanupCompleted / UserAccountDeleted）。
    /// 使用共享 durable（AccountCleanupConsumerName），供 Server Saga 等对账消费方使用。
    /// </summary>
    IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
        CancellationToken ct = default);

    /// <summary>
    /// 发布离线推送投递命令到 NATS（RealtimeServices 调用，Gateway 消费后执行实际推送）。
    /// </summary>
    Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default);

    /// <summary>
    /// 订阅推送投递命令 subject（共享 durable consumer，Gateway 消费）。
    /// </summary>
    IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync(CancellationToken ct = default);

    /// <summary>NATS Core 发布 Typing（非 JetStream / 非 Outbox）。</summary>
    Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default);

    /// <summary>NATS Core 发布 Presence（非 JetStream / 非 Outbox）。</summary>
    Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default);

    /// <summary>每 Gateway 全量订阅 Typing（无 queue group）。</summary>
    IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(CancellationToken ct = default);

    /// <summary>每 Gateway 全量订阅 Presence（无 queue group）。</summary>
    IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(CancellationToken ct = default);

    /// <summary>向 Server 批量校验 Presence 查询目标（好友）。</summary>
    Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
        PresenceAuthorizeQuery query,
        CancellationToken ct = default);

    /// <summary>
    /// Server 侧：订阅 Presence 鉴权 request/reply 并调用 handler 回复。
    /// </summary>
    Task ServePresenceAuthorizeAsync(
        Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
        CancellationToken ct = default);

    /// <summary>
    /// 三-3：Gateway → Server：查询单个用户生命周期状态（FrozenUserCache 后台刷新）。
    /// 默认实现返回 Active（fail-open），生产路径由 NatsRealtimeMessageBus 覆盖。
    /// </summary>
    Task<UserLifecycleResponse> QueryUserLifecycleAsync(
        UserLifecycleQuery query,
        CancellationToken ct = default)
        => Task.FromResult(new UserLifecycleResponse { State = UserLifecycleState.Active });

    /// <summary>
    /// 三-3：Server 侧：订阅用户生命周期查询 request/reply 并调用 handler 回复。
    /// </summary>
    Task ServeUserLifecycleQueryAsync(
        Func<UserLifecycleQuery, CancellationToken, ValueTask<UserLifecycleResponse>> handler,
        CancellationToken ct = default)
        => Task.CompletedTask;

    Task<TimeSpan> PingAsync(CancellationToken ct = default);
}

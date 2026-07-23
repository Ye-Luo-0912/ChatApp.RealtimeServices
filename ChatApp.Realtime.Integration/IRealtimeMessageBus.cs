using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;

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
    Task<MessageRecallResult> RecallMessageAsync(
        MessageRecallCommand command,
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

    Task<TimeSpan> PingAsync(CancellationToken ct = default);
}

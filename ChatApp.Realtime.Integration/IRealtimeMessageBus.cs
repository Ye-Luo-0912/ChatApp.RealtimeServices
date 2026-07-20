using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Integration;

public interface IRealtimeMessageBus
{
    Task PublishIncomingMessageAsync(IncomingMessageCommand command, CancellationToken ct = default);
    Task PublishMessageReceiptAsync(MessageReceiptCommand command, CancellationToken ct = default);
    Task<MessageHistoryPage> QueryMessageHistoryAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default);

    /// <summary>按消息 Id 查询；UserId 须为参与方（发送或接收）。</summary>
    Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
        long userId,
        string messageId,
        CancellationToken ct = default);

    Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default);
    IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(CancellationToken ct = default);
    Task<TimeSpan> PingAsync(CancellationToken ct = default);
}

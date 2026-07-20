using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeMessageHistoryStore
{
    Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default);

    /// <summary>按消息 Id 读取单条（审核证据等）；不存在返回 null。</summary>
    Task<RealtimeHistoryMessage?> TryGetByIdAsync(string messageId, CancellationToken ct = default);
}

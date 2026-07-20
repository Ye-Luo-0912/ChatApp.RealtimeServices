using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeMessageHistoryStore : IRealtimeMessageHistoryStore
{
    public Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法查询历史消息。");
    }

    public Task<RealtimeHistoryMessage?> TryGetByIdAsync(string messageId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            "未配置真实消息存储，无法按 Id 查询消息。");
    }
}

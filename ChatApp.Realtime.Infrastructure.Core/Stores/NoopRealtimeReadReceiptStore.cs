using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 三-1/2/3/4：空实现，用于未配置 PostgreSQL 时的回退。
/// </summary>
public sealed class NoopRealtimeReadReceiptStore : IRealtimeReadReceiptStore
{
    public static NoopRealtimeReadReceiptStore Instance { get; } = new();

    private NoopRealtimeReadReceiptStore() { }

    public Task<MessageReaderPage> GetReadersAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        long? cursor,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new MessageReaderPage
        {
            Readers = Array.Empty<MessageReader>(),
            NextCursor = null,
            HasMore = false
        });
    }

    public Task<MessageReadSummary> GetReadSummaryAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new MessageReadSummary
        {
            ReadCount = 0,
            TotalMemberCount = 0,
            IsSmallGroup = true
        });
    }
}
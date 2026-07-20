using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageHistoryQueryProcessorTests
{
    [Fact]
    public async Task ProcessAsync_ClampsLimitAndReturnsStableCursor()
    {
        var store = new CapturingHistoryStore(
            CreateMessages(101, content: "hello"));
        var processor = new DefaultMessageHistoryQueryProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-1",
            UserId = 42,
            Limit = 500
        });

        Assert.True(page.Succeeded);
        Assert.Equal(100, page.Items.Count);
        Assert.True(page.HasMore);
        Assert.NotNull(page.NextCursor);
        Assert.Equal(page.Items[^1].MessageId, page.NextCursor.MessageId);
        Assert.Equal(101, store.Take);
        Assert.Equal(42, store.UserId);
    }

    [Fact]
    public async Task ProcessAsync_RejectsPartialCursorBeforeStorage()
    {
        var store = new CapturingHistoryStore([]);
        var processor = new DefaultMessageHistoryQueryProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-2",
            UserId = 42,
            BeforeReceivedAtMs = 123,
            Limit = 20
        });

        Assert.False(page.Succeeded);
        Assert.Equal("invalid_cursor", page.ErrorCode);
        Assert.False(store.Called);
    }

    [Fact]
    public async Task ProcessAsync_StopsAtResponseByteBudget()
    {
        var store = new CapturingHistoryStore(
            CreateMessages(3, new string('中', 65_536)));
        var processor = new DefaultMessageHistoryQueryProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-3",
            UserId = 42,
            Limit = 3
        });

        Assert.True(page.Succeeded);
        Assert.Single(page.Items);
        Assert.True(page.HasMore);
        Assert.NotNull(page.NextCursor);
    }

    private static IReadOnlyList<RealtimeHistoryMessage> CreateMessages(
        int count,
        string content) =>
        Enumerable.Range(0, count)
            .Select(index => new RealtimeHistoryMessage
            {
                MessageId = $"{count - index:D4}",
                ClientMessageId = $"client-{index}",
                SenderUserId = 42,
                ReceiverUserId = 43,
                Content = content,
                ReceivedAtMs = 10_000 - index
            })
            .ToArray();

    private sealed class CapturingHistoryStore : IRealtimeMessageHistoryStore
    {
        private readonly IReadOnlyList<RealtimeHistoryMessage> _messages;

        public CapturingHistoryStore(
            IReadOnlyList<RealtimeHistoryMessage> messages)
        {
            _messages = messages;
        }

        public bool Called { get; private set; }
        public long UserId { get; private set; }
        public int Take { get; private set; }

        public Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
            long userId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default)
        {
            Called = true;
            UserId = userId;
            Take = take;
            return Task.FromResult(_messages);
        }

        public Task<RealtimeHistoryMessage?> TryGetByIdAsync(string messageId, CancellationToken ct = default)
            => Task.FromResult(_messages.FirstOrDefault(m => m.MessageId == messageId));
    }
}
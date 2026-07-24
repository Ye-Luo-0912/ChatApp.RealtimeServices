using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageHistoryQueryProcessorTests
{
    private static DefaultMessageHistoryQueryProcessor CreateProcessor(
        IRealtimeMessageHistoryStore store) =>
        new(store,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance),
            new NoopRealtimeReactionStore(NullLogger<NoopRealtimeReactionStore>.Instance));

    [Fact]
    public async Task ProcessAsync_ClampsLimitAndReturnsStableCursor()
    {
        var store = new CapturingHistoryStore(
            CreateMessages(101, content: "hello"));
        var processor = CreateProcessor(store);

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
        var processor = CreateProcessor(store);

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
        // ~35 KiB/item：两条合计超过 PackingBudgetBytes（62 KiB），一条可装入。
        var store = new CapturingHistoryStore(
            CreateMessages(3, new string('x', 35_000)));
        var processor = CreateProcessor(store);

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

        var json = System.Text.Json.JsonSerializer.Serialize(page);
        Assert.True(
            System.Text.Encoding.UTF8.GetByteCount(json)
            <= ChatApp.Realtime.Abstractions.Protocol.RealtimeWireLimits.MaximumResponseBytes);
    }

    [Fact]
    public async Task ProcessAsync_SingleMessageOverHardBudget_Fails()
    {
        var store = new CapturingHistoryStore(
            CreateMessages(1, new string('中', 65_536)));
        var processor = CreateProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-3b",
            UserId = 42,
            Limit = 1
        });

        Assert.False(page.Succeeded);
        Assert.Equal("message_too_large", page.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_ConversationHistory_RequiresMembership()
    {
        var store = new CapturingHistoryStore(CreateMessages(2, "hello"))
        {
            IsMember = false
        };
        var processor = CreateProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-4",
            UserId = 42,
            ConversationId = "dm:42:43",
            Limit = 20
        });

        Assert.False(page.Succeeded);
        Assert.Equal("forbidden", page.ErrorCode);
        Assert.True(store.ConversationQueryCalled);
        Assert.False(store.Called);
    }

    [Fact]
    public async Task ProcessAsync_ConversationHistory_UsesConversationStore()
    {
        var store = new CapturingHistoryStore(CreateMessages(3, "hello"));
        var processor = CreateProcessor(store);

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "request-5",
            UserId = 42,
            ConversationId = "dm:42:43",
            Limit = 2
        });

        Assert.True(page.Succeeded);
        Assert.Equal(2, page.Items.Count);
        Assert.True(page.HasMore);
        Assert.True(store.ConversationQueryCalled);
        Assert.False(store.Called);
        Assert.Equal("dm:42:43", store.ConversationId);
        Assert.Equal(3, store.Take);
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
        public bool ConversationQueryCalled { get; private set; }
        public long UserId { get; private set; }
        public int Take { get; private set; }
        public string? ConversationId { get; private set; }
        public bool IsMember { get; set; } = true;

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

        public Task<ConversationMessageHistoryResult> QueryByConversationAsync(
            long userId,
            string conversationId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default)
        {
            UserId = userId;
            ConversationQueryCalled = true;
            ConversationId = conversationId;
            Take = take;
            return Task.FromResult(
                IsMember
                    ? ConversationMessageHistoryResult.Ok(_messages)
                    : ConversationMessageHistoryResult.Forbidden);
        }

        public Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
            long userId,
            string conversationId,
            long afterReceivedAtMs,
            string afterMessageId,
            int take,
            CancellationToken ct = default)
        {
            UserId = userId;
            ConversationQueryCalled = true;
            ConversationId = conversationId;
            Take = take;
            return Task.FromResult(
                IsMember
                    ? ConversationMessageHistoryResult.Ok(_messages)
                    : ConversationMessageHistoryResult.Forbidden);
        }

        public Task<bool> IsConversationMemberAsync(
            long userId,
            string conversationId,
            CancellationToken ct = default)
        {
            UserId = userId;
            ConversationId = conversationId;
            return Task.FromResult(IsMember);
        }

        public Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
            long userId,
            IReadOnlyCollection<string> conversationIds,
            CancellationToken ct = default)
        {
            UserId = userId;
            IReadOnlySet<string> result = IsMember
                ? conversationIds.ToHashSet(StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal);
            return Task.FromResult(result);
        }

        public Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
            long userId,
            IReadOnlyList<HistoryCatchUpQuery> queries,
            CancellationToken ct = default)
        {
            UserId = userId;
            var map = new Dictionary<string, IReadOnlyList<RealtimeHistoryMessage>>(StringComparer.Ordinal);
            foreach (var query in queries)
            {
                map[query.ConversationId] = IsMember
                    ? _messages
                    : Array.Empty<RealtimeHistoryMessage>();
            }

            return Task.FromResult<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>>(map);
        }

        public Task<RealtimeHistoryMessage?> TryGetByIdAsync(string messageId, CancellationToken ct = default)
            => Task.FromResult(_messages.FirstOrDefault(m => m.MessageId == messageId));

        public Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
            IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<string, ResolvedSyncWatermark>>(
                new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal));
    }
}
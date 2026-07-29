using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeHistoryReactionEnricherTests
{
    [Fact]
    public async Task EnrichAsync_BatchesAndSetsReactedByMe()
    {
        var store = new FakeReactionStore(
        [
            new MessageReactionRecord
            {
                MessageId = "m1",
                UserId = 1,
                Emoji = "👍",
                CreatedAtMs = 10
            },
            new MessageReactionRecord
            {
                MessageId = "m1",
                UserId = 2,
                Emoji = "👍",
                CreatedAtMs = 11
            },
            new MessageReactionRecord
            {
                MessageId = "m1",
                UserId = 2,
                Emoji = "🔥",
                CreatedAtMs = 12
            },
            new MessageReactionRecord
            {
                MessageId = "m2",
                UserId = 9,
                Emoji = "❤️",
                CreatedAtMs = 13
            }
        ]);

        var messages = new[]
        {
            CreateMessage("m1"),
            CreateMessage("m2"),
            CreateMessage("m3")
        };

        var enriched = await RealtimeHistoryReactionEnricher.EnrichAsync(
            store,
            messages,
            viewerUserId: 1);

        Assert.Equal(3, enriched.Count);
        Assert.Equal(2, enriched[0].Reactions!.Count);
        var thumb = enriched[0].Reactions!.Single(r => r.Emoji == "👍");
        Assert.Equal(2, thumb.Count);
        Assert.True(thumb.ReactedByMe);
        var fire = enriched[0].Reactions!.Single(r => r.Emoji == "🔥");
        Assert.Equal(1, fire.Count);
        Assert.False(fire.ReactedByMe);

        var heart = Assert.Single(enriched[1].Reactions!);
        Assert.Equal("❤️", heart.Emoji);
        Assert.False(heart.ReactedByMe);
        Assert.Null(enriched[2].Reactions);
        Assert.Equal(1, store.ListCalls);
    }

    private static RealtimeHistoryMessage CreateMessage(string id) =>
        new()
        {
            MessageId = id,
            ClientMessageId = id,
            SenderUserId = 1,
            ReceiverUserId = 2,
            Content = "hi",
            ReceivedAtMs = 1,
            ChangedAtMs = 1,
            EditVersion = 1
        };

    private sealed class FakeReactionStore(IReadOnlyList<MessageReactionRecord> rows)
        : IRealtimeReactionStore
    {
        public int ListCalls { get; private set; }

        public Task<MessageReactionPersistResult> AddAsync(
            string messageId,
            long actorUserId,
            string actorSessionId,
            string emoji,
            long occurredAtMs,
            ChatApp.Realtime.Abstractions.Messaging.MessageReactionOptions options,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<MessageReactionPersistResult> RemoveAsync(
            string messageId,
            long actorUserId,
            string actorSessionId,
            string emoji,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
            IReadOnlyList<string> messageIds,
            CancellationToken ct = default)
        {
            ListCalls++;
            var set = messageIds.ToHashSet(StringComparer.Ordinal);
            return Task.FromResult<IReadOnlyList<MessageReactionRecord>>(
                rows.Where(r => set.Contains(r.MessageId)).ToArray());
        }

        public Task<int> DeleteByUserAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(0);
    }
}

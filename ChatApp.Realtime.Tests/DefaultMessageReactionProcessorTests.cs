using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageReactionProcessorTests
{
    [Fact]
    public async Task ProcessAsync_Add_AppliesAndNotifies()
    {
        var store = new FakeStore
        {
            NextAdd = new MessageReactionPersistResult(
                MessageReactionPersistStatus.Applied,
                "msg-1",
                ConversationId: "dm:1:2",
                Emoji: "👍",
                OccurredAtMs: 1_000,
                EmojiCount: 1)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics);

        var result = await processor.ProcessAsync(ValidAddCommand());

        Assert.True(result.Succeeded);
        Assert.Equal("👍", result.Emoji);
        Assert.Equal(1, result.EmojiCount);
        Assert.Equal(MessageReactionAction.Add, result.Action);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_Add_IdempotentDoesNotNotify()
    {
        var store = new FakeStore
        {
            NextAdd = new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                "msg-1",
                ConversationId: "dm:1:2",
                Emoji: "👍",
                OccurredAtMs: 1_000,
                EmojiCount: 1)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics);

        var result = await processor.ProcessAsync(ValidAddCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(0, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_Remove_MissingStillSucceeds()
    {
        var store = new FakeStore
        {
            NextRemove = new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                "msg-1",
                ConversationId: "dm:1:2",
                Emoji: "👍",
                OccurredAtMs: 1_000,
                EmojiCount: 0)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics);

        var result = await processor.ProcessAsync(ValidRemoveCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(0, result.EmojiCount);
        Assert.Equal(0, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_Unauthorized_Fails()
    {
        var store = new FakeStore
        {
            NextAdd = new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics);

        var result = await processor.ProcessAsync(ValidAddCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("reaction_not_allowed", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_Recalled_Fails()
    {
        var store = new FakeStore
        {
            NextAdd = new MessageReactionPersistResult(
                MessageReactionPersistStatus.AlreadyRecalled,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics);

        var result = await processor.ProcessAsync(ValidAddCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("message_recalled", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_LimitExceeded_Fails()
    {
        var store = new FakeStore
        {
            NextAdd = new MessageReactionPersistResult(
                MessageReactionPersistStatus.LimitExceeded,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics);

        var result = await processor.ProcessAsync(ValidAddCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("reaction_limit_exceeded", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_InvalidEmoji_Fails()
    {
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(
            new FakeStore(),
            new RecordingRealtimeOutboxSignal(),
            metrics);

        var result = await processor.ProcessAsync(new MessageReactionCommand
        {
            RequestId = "req-1",
            MessageId = "msg-1",
            Emoji = new string('x', 33),
            Action = MessageReactionAction.Add,
            ActorUserId = 1,
            ActorSessionId = "sess-1",
            OccurredAtMs = 1_000
        });

        Assert.False(result.Succeeded);
        Assert.Equal("invalid_emoji", result.ErrorCode);
    }

    private static DefaultMessageReactionProcessor CreateProcessor(
        FakeStore store,
        RecordingRealtimeOutboxSignal signal,
        RealtimeMetrics metrics) =>
        new(
            store,
            signal,
            metrics,
            NullLogger<DefaultMessageReactionProcessor>.Instance,
            NoopTombstoneAndLedger.Tombstone,
            new MessageReactionOptions());

    private static MessageReactionCommand ValidAddCommand() =>
        new()
        {
            RequestId = "req-1",
            MessageId = "msg-1",
            Emoji = "👍",
            Action = MessageReactionAction.Add,
            ActorUserId = 1,
            ActorSessionId = "sess-1",
            OccurredAtMs = 1_000
        };

    private static MessageReactionCommand ValidRemoveCommand() =>
        new()
        {
            RequestId = "req-1",
            MessageId = "msg-1",
            Emoji = "👍",
            Action = MessageReactionAction.Remove,
            ActorUserId = 1,
            ActorSessionId = "sess-1",
            OccurredAtMs = 1_000
        };

    private sealed class FakeStore : IRealtimeReactionStore
    {
        public MessageReactionPersistResult NextAdd { get; set; } =
            new(MessageReactionPersistStatus.NotFound, "msg");

        public MessageReactionPersistResult NextRemove { get; set; } =
            new(MessageReactionPersistStatus.NotFound, "msg");

        public Task<MessageReactionPersistResult> AddAsync(
            string messageId,
            long actorUserId,
            string actorSessionId,
            string emoji,
            long occurredAtMs,
            MessageReactionOptions options,
            CancellationToken ct = default) =>
            Task.FromResult(NextAdd);

        public Task<MessageReactionPersistResult> RemoveAsync(
            string messageId,
            long actorUserId,
            string actorSessionId,
            string emoji,
            long occurredAtMs,
            CancellationToken ct = default) =>
            Task.FromResult(NextRemove);

        public Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
            IReadOnlyList<string> messageIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<MessageReactionRecord>>([]);
    }
}

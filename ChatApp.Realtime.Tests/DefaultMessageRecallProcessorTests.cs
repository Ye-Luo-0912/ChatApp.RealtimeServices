using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageRecallProcessorTests
{
    [Fact]
    public async Task ProcessAsync_AppliesRecall()
    {
        var store = new FakeStore
        {
            Next = new MessageRecallPersistResult(
                MessageRecallPersistStatus.Applied,
                "msg-1",
                ReceiverUserId: 2,
                ConversationId: "dm:1:2",
                RecalledAtMs: 1_000)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics, maxAgeMinutes: 2);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(1_000, result.RecalledAtMs);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_Unauthorized_Fails()
    {
        var store = new FakeStore
        {
            Next = new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics, maxAgeMinutes: 2);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("recall_not_allowed", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_WindowExpired_Fails()
    {
        var store = new FakeStore
        {
            Next = new MessageRecallPersistResult(
                MessageRecallPersistStatus.WindowExpired,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics, maxAgeMinutes: 2);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("recall_window_expired", result.ErrorCode);
    }

    private static DefaultMessageRecallProcessor CreateProcessor(
        FakeStore store,
        RecordingRealtimeOutboxSignal signal,
        RealtimeMetrics metrics,
        int maxAgeMinutes) =>
        new(
            store,
            signal,
            metrics,
            NullLogger<DefaultMessageRecallProcessor>.Instance,
            new MessageRecallOptions { MaxAgeMinutes = maxAgeMinutes });

    private static MessageRecallCommand ValidCommand() =>
        new()
        {
            RequestId = "req-1",
            MessageId = "msg-1",
            SenderUserId = 1,
            SenderSessionId = "sess-1",
            OccurredAtMs = 1_000
        };

    private sealed class FakeStore : IRealtimeMessageStore
    {
        public MessageRecallPersistResult Next { get; set; } =
            new(MessageRecallPersistStatus.NotFound, "msg");

        public Task<RealtimeMessagePersistResult> SaveAsync(
            RealtimeMessageRecord message,
            ChatApp.Realtime.Abstractions.Events.RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            ChatApp.Realtime.Abstractions.Events.RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<MessageRecallPersistResult> ApplyRecallAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            long recalledAtMs,
            long maxAgeMs,
            CancellationToken ct = default) =>
            Task.FromResult(Next);

        public Task<MessageEditPersistResult> ApplyEditAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            string content,
            long editedAtMs,
            long maxAgeMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<long> DeleteByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default) =>
            Task.FromResult(0L);

        public Task EnqueueEventAsync(
            ChatApp.Realtime.Abstractions.Events.RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.CompletedTask;
    }
}

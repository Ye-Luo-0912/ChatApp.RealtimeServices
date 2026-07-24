using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageEditProcessorTests
{
    [Fact]
    public async Task ProcessAsync_AppliesEditAndBumpsVersion()
    {
        var store = new FakeStore
        {
            Next = new MessageEditPersistResult(
                MessageEditPersistStatus.Applied,
                "msg-1",
                ReceiverUserId: 2,
                ConversationId: "dm:1:2",
                Content: "hello-v2",
                EditVersion: 2,
                EditedAtMs: 1_000)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics, maxAgeMinutes: 15);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(2, result.EditVersion);
        Assert.Equal("hello-v2", result.Content);
        Assert.Equal(1, signal.Notifications);
        Assert.Equal("req-1", store.LastRequestId);
    }

    [Fact]
    public async Task ProcessAsync_WindowExpired_Fails()
    {
        var store = new FakeStore
        {
            Next = new MessageEditPersistResult(
                MessageEditPersistStatus.WindowExpired,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics, maxAgeMinutes: 15);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("edit_window_expired", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_Unauthorized_Fails()
    {
        var store = new FakeStore
        {
            Next = new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                "msg-1")
        };
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, new RecordingRealtimeOutboxSignal(), metrics, maxAgeMinutes: 15);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("edit_not_allowed", result.ErrorCode);
    }

    [Fact]
    public async Task ProcessAsync_DuplicateRequest_ReturnsSameResultWithoutNotify()
    {
        var store = new FakeStore
        {
            Next = new MessageEditPersistResult(
                MessageEditPersistStatus.Unchanged,
                "msg-1",
                ReceiverUserId: 2,
                ConversationId: "dm:1:2",
                Content: "hello-v2",
                EditVersion: 2,
                EditedAtMs: 1_000)
        };
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, signal, metrics, maxAgeMinutes: 15);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(2, result.EditVersion);
        Assert.Equal(0, signal.Notifications);
    }

    private static DefaultMessageEditProcessor CreateProcessor(
        FakeStore store,
        RecordingRealtimeOutboxSignal signal,
        RealtimeMetrics metrics,
        int maxAgeMinutes) =>
        new(
            store,
            signal,
            metrics,
            NullLogger<DefaultMessageEditProcessor>.Instance,
            new MessageEditOptions { MaxAgeMinutes = maxAgeMinutes });

    private static MessageEditCommand ValidCommand() =>
        new()
        {
            RequestId = "req-1",
            MessageId = "msg-1",
            Content = "hello-v2",
            SenderUserId = 1,
            SenderSessionId = "sess-1",
            OccurredAtMs = 1_000
        };

    private sealed class FakeStore : IRealtimeMessageStore
    {
        public MessageEditPersistResult Next { get; set; } =
            new(MessageEditPersistStatus.NotFound, "msg");

        public string? LastRequestId { get; private set; }

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
            throw new NotSupportedException();

        public Task<MessageEditPersistResult> ApplyEditAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            string content,
            long editedAtMs,
            long maxAgeMs,
            CancellationToken ct = default)
        {
            LastRequestId = requestId;
            return Task.FromResult(Next);
        }

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

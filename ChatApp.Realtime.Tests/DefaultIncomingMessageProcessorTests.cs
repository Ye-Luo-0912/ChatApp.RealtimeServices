using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultIncomingMessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_PersistsMessageAndOutboxEventTogether()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);
        var command = ValidCommand();

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.Equal(command.CommandId, result.MessageId);
        Assert.Equal(1, signal.Notifications);
        Assert.NotNull(store.Message);
        Assert.NotNull(store.Event);
        Assert.Equal(command.CommandId, store.Event.MessageId);
        Assert.Equal(RealtimeEventType.MessageReceived, store.Event.Type);
        Assert.Equal(command.ReceiverUserId, store.Event.TargetUserId);
        var payload = RealtimeWireSerializer.DeserializeChatMessage(store.Event.PayloadJson!);
        Assert.NotNull(payload);
        Assert.Equal(command.Content, payload.Content);
        Assert.Equal(command.ClientMessageId, payload.ClientMessageId);
    }

    [Fact]
    public async Task ProcessAsync_PersistsCurrentTraceContextInOutboxEvent()
    {
        using var activity = new Activity("test")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();
        var store = new CapturingStore();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            new RecordingRealtimeOutboxSignal(),
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        await processor.ProcessAsync(ValidCommand());

        Assert.Equal(activity.Id, store.Event!.TraceParent);
        Assert.Equal(activity.TraceStateString, store.Event.TraceState);
    }

    [Fact]
    public async Task ProcessAsync_CreatesDeterministicEventIdForRetries()
    {
        var firstStore = new CapturingStore();
        var secondStore = new CapturingStore();
        using var firstMetrics = new RealtimeMetrics();
        using var secondMetrics = new RealtimeMetrics();
        var command = ValidCommand();

        await new DefaultIncomingMessageProcessor(
            firstStore,
            new RecordingRealtimeOutboxSignal(),
            firstMetrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance).ProcessAsync(command);
        await new DefaultIncomingMessageProcessor(
            secondStore,
            new RecordingRealtimeOutboxSignal(),
            secondMetrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance).ProcessAsync(command);

        Assert.Equal(firstStore.Event!.EventId, secondStore.Event!.EventId);
        Assert.Equal(64, firstStore.Event.EventId.Length);
    }

    [Fact]
    public async Task ProcessAsync_NotifiesOutboxForIdempotentMessage()
    {
        var store = new CapturingStore(isNew: false);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_RejectsPermanentValidationErrorBeforeStorage()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);
        var command = ValidCommand() with { Content = " " };

        var result = await processor.ProcessAsync(command);

        Assert.False(result.Succeeded);
        Assert.Equal(MessageFailureKind.Permanent, result.FailureKind);
        Assert.Equal("empty_content", result.ErrorCode);
        Assert.Equal(0, signal.Notifications);
        Assert.Null(store.Message);
    }

    private static IncomingMessageCommand ValidCommand() => new()
    {
        CommandId = "command-1",
        ClientMessageId = "client-1",
        SenderUserId = 1001,
        SenderSessionId = "session-1",
        ReceiverUserId = 1002,
        Content = "hello",
        ReceivedAtMs = 1_700_000_000_000
    };

    private sealed class CapturingStore(bool isNew = true) : IRealtimeMessageStore
    {
        public RealtimeMessageRecord? Message { get; private set; }
        public RealtimeEvent? Event { get; private set; }

        public Task<RealtimeMessagePersistResult> SaveAsync(
            RealtimeMessageRecord message,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default)
        {
            Message = message;
            Event = eventToPublish;
            return Task.FromResult(new RealtimeMessagePersistResult(isNew, message.MessageId));
        }
        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId));
        public Task<long> DeleteByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default) =>
            Task.FromResult(0L);

        public Task EnqueueEventAsync(
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.CompletedTask;
    }
}

using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultMessageReceiptProcessorTests
{
    [Fact]
    public async Task ProcessAsync_PersistsReceiptAndOutboxEventTogether()
    {
        var store = new CapturingStore(
            MessageReceiptPersistStatus.Applied);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, metrics, signal);
        var command = ValidCommand();

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.Equal(1, signal.Notifications);
        Assert.NotNull(store.Receipt);
        Assert.NotNull(store.Event);
        Assert.Equal(
            RealtimeEventType.MessageReceiptUpdated,
            store.Event.Type);
        Assert.Equal(0, store.Event.TargetUserId);
        var payload = RealtimeWireSerializer
            .DeserializeMessageReceipt(store.Event.PayloadJson!);
        Assert.NotNull(payload);
        Assert.Equal(command.MessageId, payload.MessageId);
        Assert.Equal(command.ReceiverUserId, payload.ReceiverUserId);
        Assert.Equal(command.ReceiptType, payload.ReceiptType);
    }

    [Fact]
    public async Task ProcessAsync_TreatsDuplicateReceiptAsSuccess()
    {
        var store = new CapturingStore(
            MessageReceiptPersistStatus.Unchanged);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, metrics, signal);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal("message-1", result.MessageId);
        Assert.Equal(0, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_RejectsReceiverMismatchPermanently()
    {
        var store = new CapturingStore(
            MessageReceiptPersistStatus.ReceiverMismatch);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = CreateProcessor(store, metrics, signal);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal(MessageFailureKind.Permanent, result.FailureKind);
        Assert.Equal("receipt_not_allowed", result.ErrorCode);
        Assert.Equal(0, signal.Notifications);
    }

    private static DefaultMessageReceiptProcessor CreateProcessor(
        IRealtimeMessageStore store,
        RealtimeMetrics metrics,
        IRealtimeOutboxSignal signal) =>
        new(
            store,
            signal,
            metrics,
            NullLogger<DefaultMessageReceiptProcessor>.Instance);

    private static MessageReceiptCommand ValidCommand() => new()
    {
        CommandId = new string('a', 64),
        MessageId = "message-1",
        ReceiverUserId = 1002,
        ReceiverSessionId = "receiver-session",
        ReceiptType = MessageReceiptType.Read,
        OccurredAtMs = 1_700_000_000_100
    };

    private sealed class CapturingStore(
        MessageReceiptPersistStatus status) : IRealtimeMessageStore
    {
        public MessageReceiptRecord? Receipt { get; private set; }
        public RealtimeEvent? Event { get; private set; }

        public Task<RealtimeMessagePersistResult> SaveAsync(
            RealtimeMessageRecord message,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                RealtimeMessagePersistResult.Created(message.MessageId));

        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default)
        {
            Receipt = receipt;
            Event = eventToPublish;
            return Task.FromResult(
                new MessageReceiptPersistResult(
                    status,
                    receipt.MessageId,
                    1001));
        }

        public Task<MessageRecallPersistResult> ApplyRecallAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            long recalledAtMs,
            long maxAgeMs,
            CancellationToken ct = default) =>
            Task.FromResult(new MessageRecallPersistResult(MessageRecallPersistStatus.NotFound, messageId));

        public Task<MessageEditPersistResult> ApplyEditAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            string content,
            long editedAtMs,
            long maxAgeMs,
            IReadOnlyList<long>? mentionedUserIds = null,
            IReadOnlyList<string>? mentionedRoles = null,
            CancellationToken ct = default) =>
            Task.FromResult(new MessageEditPersistResult(MessageEditPersistStatus.NotFound, messageId));

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

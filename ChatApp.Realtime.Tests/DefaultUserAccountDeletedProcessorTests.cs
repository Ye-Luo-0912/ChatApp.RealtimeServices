using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultUserAccountDeletedProcessorTests
{
    [Fact]
    public async Task Process_UserAccountDeleted_DeletesByUser_AndEnqueuesCompleted()
    {
        var store = new InMemoryUserStore();
        for (var i = 0; i < 1500; i++)
            store.Add(42, i);
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = new DefaultUserAccountDeletedProcessor(
            store, signal, NullLogger<DefaultUserAccountDeletedProcessor>.Instance);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e1",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 42,
            OccurredAtMs = 1,
        });

        Assert.True(result.Succeeded);
        Assert.Equal(1500, store.LastDeletedCount);
        Assert.Equal(0, store.RemainingFor(42));
        Assert.Single(store.Enqueued);
        Assert.Equal(RealtimeEventType.AccountCleanupCompleted, store.Enqueued[0].Type);
        Assert.Equal("cleanup-done:e1", store.Enqueued[0].EventId);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task Process_Duplicate_IsIdempotent()
    {
        var store = new InMemoryUserStore();
        store.Add(7, 1);
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = new DefaultUserAccountDeletedProcessor(
            store, signal, NullLogger<DefaultUserAccountDeletedProcessor>.Instance);
        var evt = new RealtimeEvent
        {
            EventId = "e2",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 7,
            OccurredAtMs = 1,
        };

        Assert.True((await processor.ProcessAsync(evt)).Succeeded);
        Assert.True((await processor.ProcessAsync(evt)).Succeeded);
        Assert.Equal(2, store.DeleteCalls);
        Assert.Equal(2, store.Enqueued.Count);
        Assert.Equal(2, signal.Notifications);
    }

    [Fact]
    public async Task Process_TransientFailure_ThenSucceeds_OnRetry()
    {
        var store = new InMemoryUserStore { FailNextDeletes = 1 };
        store.Add(9, 1);
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = new DefaultUserAccountDeletedProcessor(
            store, signal, NullLogger<DefaultUserAccountDeletedProcessor>.Instance);
        var evt = new RealtimeEvent
        {
            EventId = "e-nak",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 9,
            OccurredAtMs = 1,
        };

        var first = await processor.ProcessAsync(evt);
        Assert.False(first.Succeeded);
        Assert.Equal("cleanup_transient", first.ErrorCode);
        Assert.Empty(store.Enqueued);
        Assert.Equal(0, signal.Notifications);

        var second = await processor.ProcessAsync(evt);
        Assert.True(second.Succeeded);
        Assert.Single(store.Enqueued);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task Process_OtherEventTypes_AreNoOp()
    {
        var store = new InMemoryUserStore();
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = new DefaultUserAccountDeletedProcessor(
            store, signal, NullLogger<DefaultUserAccountDeletedProcessor>.Instance);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e3",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 1,
            OccurredAtMs = 1,
        });

        Assert.True(result.Succeeded);
        Assert.Equal(0, store.DeleteCalls);
        Assert.Empty(store.Enqueued);
        Assert.Equal(0, signal.Notifications);
    }

    private sealed class InMemoryUserStore : IRealtimeMessageStore
    {
        private readonly List<(long UserId, int Id)> _rows = [];
        public int DeleteCalls { get; private set; }
        public long LastDeletedCount { get; private set; }
        public int FailNextDeletes { get; set; }
        public List<RealtimeEvent> Enqueued { get; } = [];

        public void Add(long userId, int id) => _rows.Add((userId, id));

        public int RemainingFor(long userId) => _rows.Count(r => r.UserId == userId);

        public Task<RealtimeMessagePersistResult> SaveAsync(
            RealtimeMessageRecord message,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                new RealtimeMessagePersistResult(true, message.MessageId));

        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId));

        public Task<long> DeleteByUserAsync(
            long userId, int batchSize = 1000, CancellationToken ct = default)
        {
            DeleteCalls++;
            if (FailNextDeletes > 0)
            {
                FailNextDeletes--;
                throw new InvalidOperationException("simulated crash mid-cleanup");
            }

            long deleted = 0;
            while (true)
            {
                var batch = _rows.Where(r => r.UserId == userId).Take(batchSize).ToList();
                if (batch.Count == 0)
                    break;
                foreach (var row in batch)
                    _rows.Remove(row);
                deleted += batch.Count;
            }

            LastDeletedCount = deleted;
            return Task.FromResult(deleted);
        }

        public Task EnqueueEventAsync(RealtimeEvent eventToPublish, CancellationToken ct = default)
        {
            Enqueued.Add(eventToPublish);
            return Task.CompletedTask;
        }
    }
}

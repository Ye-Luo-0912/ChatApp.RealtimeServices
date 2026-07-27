using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultUserAccountDeletedProcessorTests
{
    private static DefaultUserAccountDeletedProcessor CreateProcessor(
        IRealtimeMessageStore store,
        IRealtimeAttachmentStore attachments,
        IRealtimeOutboxSignal signal,
        IRealtimeDeviceSyncCursorStore? cursors = null) =>
        new(
            store,
            attachments,
            cursors ?? new NoopRealtimeDeviceSyncCursorStore(),
            NoopTombstoneAndLedger.Tombstone,
            signal,
            NullLogger<DefaultUserAccountDeletedProcessor>.Instance);

    [Fact]
    public async Task Process_UserAccountDeleted_DeletesByUser_AndEnqueuesCompleted()
    {
        var store = new InMemoryUserStore();
        for (var i = 0; i < 1500; i++)
            store.Add(42, i);
        var attachments = new InMemoryAttachmentStore();
        attachments.Add(42, "obj/a");
        attachments.Add(42, "obj/b");
        var cursors = new RecordingDeviceCursorStore();
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = CreateProcessor(store, attachments, signal, cursors);

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
        Assert.Equal(2, store.Enqueued.Count);
        Assert.Equal(RealtimeEventType.AttachmentBlobsPurge, store.Enqueued[0].Type);
        Assert.Equal(RealtimeEventType.AccountCleanupCompleted, store.Enqueued[1].Type);
        Assert.Equal("cleanup-done:e1", store.Enqueued[1].EventId);
        Assert.Equal(1, signal.Notifications);
        Assert.Empty(attachments.RemainingKeys(42));
        Assert.Equal(1, cursors.DeleteByUserCalls);
        Assert.Equal(42, cursors.LastDeletedUserId);
    }

    [Fact]
    public async Task Process_Duplicate_IsIdempotent()
    {
        var store = new InMemoryUserStore();
        store.Add(7, 1);
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = CreateProcessor(store, new InMemoryAttachmentStore(), signal);
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
    public async Task Process_CrashAfterPurgeEnqueue_RetryStillCompletes()
    {
        var store = new InMemoryUserStore();
        store.Add(99, 1);
        var attachments = new InMemoryAttachmentStore { FailNextDeletes = 1 };
        attachments.Add(99, "obj/orphan-a");
        attachments.Add(99, "obj/orphan-b");
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = CreateProcessor(store, attachments, signal);
        var evt = new RealtimeEvent
        {
            EventId = "e-orphan",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 99,
            OccurredAtMs = 1,
        };

        // 第一次：purge 已入队，随后在附件元数据删除处崩溃；键仍在库中。
        var first = await processor.ProcessAsync(evt);
        Assert.False(first.Succeeded);
        Assert.Equal("cleanup_transient", first.ErrorCode);
        Assert.Contains(store.Enqueued, e => e.Type == RealtimeEventType.AttachmentBlobsPurge);
        Assert.Equal(2, attachments.RemainingKeys(99).Count);
        Assert.Equal(0, signal.Notifications);

        // 第二次：仍能 list 到相同 keys（幂等 EventId），完成删除与 AccountCleanupCompleted。
        var second = await processor.ProcessAsync(evt);
        Assert.True(second.Succeeded);
        Assert.Empty(attachments.RemainingKeys(99));
        Assert.Contains(store.Enqueued, e => e.Type == RealtimeEventType.AccountCleanupCompleted);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task Process_TransientFailure_ThenSucceeds_OnRetry()
    {
        var store = new InMemoryUserStore { FailNextDeletes = 1 };
        store.Add(9, 1);
        var signal = new RecordingRealtimeOutboxSignal();
        var processor = CreateProcessor(store, new InMemoryAttachmentStore(), signal);
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
        var processor = CreateProcessor(store, new InMemoryAttachmentStore(), signal);

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

    private sealed class RecordingDeviceCursorStore : IRealtimeDeviceSyncCursorStore
    {
        public int DeleteByUserCalls { get; private set; }
        public long LastDeletedUserId { get; private set; }

        public Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
            long userId,
            ulong deviceIdHash,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<DeviceSyncCursor>>([]);

        public Task UpsertManyAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<DeviceSyncCursor> cursors,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task DeleteAsync(
            long userId,
            ulong deviceIdHash,
            IReadOnlyList<string> conversationIds,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default)
        {
            DeleteByUserCalls++;
            LastDeletedUserId = userId;
            return Task.FromResult(3L);
        }

        public Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }

    private sealed class InMemoryAttachmentStore : IRealtimeAttachmentStore
    {
        private readonly List<(long UserId, string ObjectKey)> _rows = [];
        public int FailNextDeletes { get; set; }

        public void Add(long userId, string objectKey) => _rows.Add((userId, objectKey));

        public IReadOnlyList<string> RemainingKeys(long userId) =>
            _rows.Where(r => r.UserId == userId).Select(r => r.ObjectKey).ToArray();

        public Task<RealtimeAttachmentRecord> InsertConfirmedAsync(
            RealtimeAttachmentRecord attachment,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<int> BindToMessageAsync(
            string messageId,
            string? conversationId,
            long uploaderUserId,
            IReadOnlyList<string> attachmentIds,
            CancellationToken ct = default) =>
            Task.FromResult(0);

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(
            IReadOnlyList<string> messageIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>([]);

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListForUserExportAsync(
            long userId,
            string? afterAttachmentId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>([]);

        public Task<IReadOnlyList<string>> ListObjectKeysByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<string>>(
                _rows.Where(r => r.UserId == userId).Select(r => r.ObjectKey).ToArray());

        public Task<IReadOnlyList<string>> DeleteByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default)
        {
            if (FailNextDeletes > 0)
            {
                FailNextDeletes--;
                throw new InvalidOperationException("simulated crash after purge enqueue");
            }

            var keys = _rows.Where(r => r.UserId == userId).Select(r => r.ObjectKey).ToList();
            _rows.RemoveAll(r => r.UserId == userId);
            return Task.FromResult<IReadOnlyList<string>>(keys);
        }
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
                RealtimeMessagePersistResult.Created(message.MessageId));

        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId));

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
            CancellationToken ct = default) =>
            Task.FromResult(new MessageEditPersistResult(MessageEditPersistStatus.NotFound, messageId));

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

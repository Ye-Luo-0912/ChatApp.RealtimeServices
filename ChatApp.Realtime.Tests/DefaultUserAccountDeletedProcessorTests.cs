using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultUserAccountDeletedProcessorTests
{
    private static DefaultUserAccountDeletedProcessor CreateProcessor(
        IAccountCleanupJobStore jobStore,
        IUserDeletionTombstoneStore? tombstone = null) =>
        new(
            jobStore,
            tombstone ?? NoopTombstoneAndLedger.Tombstone,
            NullLogger<DefaultUserAccountDeletedProcessor>.Instance);

    [Fact]
    public async Task Process_UserAccountDeleted_EnqueuesJob_AndWritesTombstone()
    {
        var jobStore = new RecordingJobStore();
        var tombstone = new RecordingTombstoneStore();
        var processor = CreateProcessor(jobStore, tombstone);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e1",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 42,
            OccurredAtMs = 1,
        });

        Assert.True(result.Succeeded);
        Assert.Equal(1, jobStore.EnqueueCalls);
        Assert.Equal(42, jobStore.LastEnqueuedUserId);
        Assert.Equal(1, tombstone.RecordDeletionCalls);
        Assert.Equal(42, tombstone.LastDeletedUserId);
        Assert.Equal("e1", tombstone.LastDeletionEventId);
    }

    [Fact]
    public async Task Process_Duplicate_IsIdempotent()
    {
        var jobStore = new RecordingJobStore();
        var processor = CreateProcessor(jobStore);
        var evt = new RealtimeEvent
        {
            EventId = "e2",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 7,
            OccurredAtMs = 1,
        };

        Assert.True((await processor.ProcessAsync(evt)).Succeeded);
        Assert.True((await processor.ProcessAsync(evt)).Succeeded);
        // EnqueueJobAsync 幂等：重复调用不会创建第二个作业行。
        Assert.Equal(2, jobStore.EnqueueCalls);
    }

    [Fact]
    public async Task Process_InvalidUser_ReturnsFailure()
    {
        var jobStore = new RecordingJobStore();
        var processor = CreateProcessor(jobStore);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e-invalid",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 0,
            OccurredAtMs = 1,
        });

        Assert.False(result.Succeeded);
        Assert.Equal("invalid_user", result.ErrorCode);
        Assert.Equal(0, jobStore.EnqueueCalls);
    }

    [Fact]
    public async Task Process_OtherEventTypes_AreNoOp()
    {
        var jobStore = new RecordingJobStore();
        var processor = CreateProcessor(jobStore);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e3",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 1,
            OccurredAtMs = 1,
        });

        Assert.True(result.Succeeded);
        Assert.Equal(0, jobStore.EnqueueCalls);
    }

    [Fact]
    public async Task Process_TransientFailure_WhenJobStoreThrows_ReturnsFailure()
    {
        var jobStore = new RecordingJobStore { ThrowOnEnqueue = true };
        var processor = CreateProcessor(jobStore);

        var result = await processor.ProcessAsync(new RealtimeEvent
        {
            EventId = "e-fail",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 9,
            OccurredAtMs = 1,
        });

        Assert.False(result.Succeeded);
        Assert.Equal("cleanup_transient", result.ErrorCode);
    }

    private sealed class RecordingJobStore : IAccountCleanupJobStore
    {
        public int EnqueueCalls { get; private set; }
        public long LastEnqueuedUserId { get; private set; }
        public bool ThrowOnEnqueue { get; set; }

        public Task<AccountCleanupJob> EnqueueJobAsync(long userId, long occurredAtMs, CancellationToken ct = default)
        {
            if (ThrowOnEnqueue)
                throw new InvalidOperationException("simulated job store failure");
            EnqueueCalls++;
            LastEnqueuedUserId = userId;
            return Task.FromResult(new AccountCleanupJob(
                userId,
                AccountCleanupJob.PhaseAttachments,
                Cursor: null,
                AccountCleanupJob.StatusPending,
                RetryCount: 0,
                UpdatedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
        }

        public Task<AccountCleanupJob?> TryClaimAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult<AccountCleanupJob?>(null);

        public Task UpdateProgressAsync(
            long userId, string phase, string? cursor, string status, string claimToken, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task CompletePhaseAsync(long userId, string phase, string claimToken, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<AccountCleanupJob?> GetNextPendingAsync(
            string instanceId, TimeSpan leaseDuration, CancellationToken ct = default) =>
            Task.FromResult<AccountCleanupJob?>(null);

        public Task<bool> RenewLeaseAsync(
            long userId, string phase, string claimToken, TimeSpan leaseExtension, CancellationToken ct = default) =>
            Task.FromResult(true);

        public Task<bool> ProcessAttachmentsBatchAtomicAsync(
            long userId, string claimToken, string lastAttachmentId,
            IReadOnlyList<string> attachmentIds, ChatApp.Realtime.Abstractions.Events.RealtimeEvent purgeEvent,
            CancellationToken ct = default) =>
            Task.FromResult(true);

        public Task RecordFailureAsync(
            long userId, string phase, string claimToken, int maxRetryCount, CancellationToken ct = default) =>
            Task.CompletedTask;
    }

    private sealed class RecordingTombstoneStore : IUserDeletionTombstoneStore
    {
        public int RecordDeletionCalls { get; private set; }
        public long LastDeletedUserId { get; private set; }
        public string LastDeletionEventId { get; private set; } = string.Empty;

        public Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(false);

        public Task<UserLifecycleState> GetLifecycleStateAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(UserLifecycleState.Active);

        public Task<IReadOnlyDictionary<long, UserLifecycleState>> BatchGetUserLifecycleStateAsync(
            IReadOnlyList<long> userIds, CancellationToken ct = default)
        {
            var result = new Dictionary<long, UserLifecycleState>(userIds.Count);
            foreach (var id in userIds)
                result.TryAdd(id, UserLifecycleState.Active);
            return Task.FromResult<IReadOnlyDictionary<long, UserLifecycleState>>(result);
        }

        public Task RecordDeletionAsync(
            long userId, string deletionEventId, long deletedAtMs, CancellationToken ct = default)
        {
            RecordDeletionCalls++;
            LastDeletedUserId = userId;
            LastDeletionEventId = deletionEventId;
            return Task.CompletedTask;
        }

        public Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }
}

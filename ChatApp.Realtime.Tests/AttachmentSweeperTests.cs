using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Attachments;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1-3：未绑定附件过期清理器测试。验证过期候选被标记为 Expired、
/// 对象存储删除被调用、空候选返回 0、并发冲突（MarkExpired 失败）跳过候选。
/// </summary>
public sealed class AttachmentSweeperTests
{
    [Fact]
    public async Task SweepAsync_ExpiresCandidates_AndDeletesObjects()
    {
        var store = new FakeSweepStore(
            Uploaded("a1", "k1", 1),
            Uploaded("a2", "k2", 1));
        var storage = new RecordingObjectStorage();
        var sweeper = new AttachmentSweeper(
            store,
            NullLogger<AttachmentSweeper>.Instance,
            retention: TimeSpan.FromDays(7),
            objectStorage: storage);

        var expired = await sweeper.SweepAsync();

        Assert.Equal(2, expired);
        Assert.Equal(2, store.ExpiredCount);
        Assert.Equal(["k1", "k2"], storage.DeletedKeys);
    }

    [Fact]
    public async Task SweepAsync_NoCandidates_ReturnsZero()
    {
        var store = new FakeSweepStore();
        var sweeper = new AttachmentSweeper(store, NullLogger<AttachmentSweeper>.Instance);

        var expired = await sweeper.SweepAsync();

        Assert.Equal(0, expired);
    }

    [Fact]
    public async Task SweepAsync_ConcurrentMarkConflict_Skips()
    {
        // 候选「a1」在列表中携带版本 3，但落库时 store 当前版本已变为 4（并发绑定/扫描）
        // → MarkExpired 返回 false，sweeper 跳过该候选，不计入过期数。
        var store = new FakeSweepStore();
        store.AddConcurrentCandidate(Uploaded("a1", "k1", 3), currentVersion: 4);
        var sweeper = new AttachmentSweeper(store, NullLogger<AttachmentSweeper>.Instance);

        var expired = await sweeper.SweepAsync();

        Assert.Equal(0, expired);
        Assert.Equal(0, store.ExpiredCount);
    }

    private static RealtimeAttachmentRecord Uploaded(string id, string key, long stateVersion) =>
        new()
        {
            AttachmentId = id,
            UploaderUserId = 10,
            ObjectKey = key,
            ContentType = "application/octet-stream",
            SizeBytes = 1,
            Status = AttachmentStatus.Uploaded,
            CreatedAtMs = 1,
            StateVersion = stateVersion
        };

    private sealed class RecordingObjectStorage : IObjectStorage
    {
        public List<string> DeletedKeys { get; } = [];

        public Task<ObjectHead?> HeadAsync(string objectKey, CancellationToken ct = default) =>
            Task.FromResult<ObjectHead?>(null);

        public Task DeleteAsync(string objectKey, CancellationToken ct = default)
        {
            DeletedKeys.Add(objectKey);
            return Task.CompletedTask;
        }

        public Task<string> CreateSignedDownloadUrlAsync(string objectKey, TimeSpan ttl, CancellationToken ct = default) =>
            Task.FromResult("https://example.local/" + objectKey);
    }

    private sealed class FakeSweepStore : IRealtimeAttachmentStore
    {
        private readonly List<(RealtimeAttachmentRecord Record, long CurrentVersion)> _candidates = [];
        private readonly HashSet<string> _expired = new(StringComparer.Ordinal);

        public FakeSweepStore(params RealtimeAttachmentRecord[] candidates)
        {
            foreach (var c in candidates)
                _candidates.Add((c, c.StateVersion));
        }

        /// <summary>模拟并发冲突：列表中携带的版本与 store 当前权威版本不一致。</summary>
        public void AddConcurrentCandidate(RealtimeAttachmentRecord record, long currentVersion) =>
            _candidates.Add((record, currentVersion));

        public int ExpiredCount => _expired.Count;

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListExpiryCandidatesAsync(
            long cutoffMs, int take, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>(_candidates.Select(x => x.Record).ToArray());

        public Task<bool> MarkExpiredAsync(string attachmentId, long expectedStateVersion, CancellationToken ct = default)
        {
            var entry = _candidates.FirstOrDefault(e => e.Record.AttachmentId == attachmentId);
            if (entry.Record is null || entry.CurrentVersion != expectedStateVersion)
                return Task.FromResult(false);
            _expired.Add(attachmentId);
            return Task.FromResult(true);
        }

        public Task<RealtimeAttachmentRecord> InsertConfirmedAsync(RealtimeAttachmentRecord attachment, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<int> BindToMessageAsync(string messageId, string? conversationId, long uploaderUserId, IReadOnlyList<string> attachmentIds, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(long actorUserId, string attachmentId, long sizeBytes, string? contentHash, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentScanTransitionResult> BeginScanAsync(string attachmentId, long expectedStateVersion, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentScanTransitionResult> CompleteScanAsync(string attachmentId, long expectedStateVersion, AttachmentScanVerdict verdict, long sizeBytes, string? contentHash, string? contentType, string? reason, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(IReadOnlyList<string> messageIds, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListForUserExportAsync(long userId, string? afterAttachmentId, int take, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<string>> ListObjectKeysByUserAsync(long userId, int batchSize = 1000, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<string>> DeleteByUserAsync(long userId, int batchSize = 1000, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<int> DeleteByAttachmentIdsAsync(IReadOnlyList<string> attachmentIds, CancellationToken ct = default) =>
            throw new NotSupportedException();
    }
}
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Attachments;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1-3：附件扫描处理器状态机测试。验证 Uploaded → Scanning → Available/Rejected 转换、
/// state_version 条件更新、HEAD 元数据一致性校验（大小/Content-Type 不符拒绝）。
/// </summary>
public sealed class AttachmentScanProcessorTests
{
    [Fact]
    public async Task Pass_NoObjectStorage_TransitionsToAvailable()
    {
        var record = Uploaded("a1", sizeBytes: 100, contentType: "image/png", stateVersion: 1);
        var store = new FakeAttachmentStore(record);
        var processor = new AttachmentScanProcessor(store, NullLogger<AttachmentScanProcessor>.Instance);

        var result = await processor.ProcessAsync(
            new AttachmentScanCommand
            {
                RequestId = "req-1",
                AttachmentId = "a1",
                Verdict = AttachmentScanVerdict.Pass,
                StateVersion = 1,
                SizeBytes = 100,
                ContentType = "image/png"
            });

        Assert.True(result.Succeeded);
        Assert.Equal((short)AttachmentStatus.Available, result.Status);
        Assert.Equal(AttachmentStatus.Available, store.Current!.Status);
    }

    [Fact]
    public async Task Reject_TransitionsToRejected()
    {
        var store = new FakeAttachmentStore(Uploaded("a1", 100, "application/octet-stream", 1));
        var processor = new AttachmentScanProcessor(store, NullLogger<AttachmentScanProcessor>.Instance);

        var result = await processor.ProcessAsync(
            new AttachmentScanCommand
            {
                RequestId = "req-2",
                AttachmentId = "a1",
                Verdict = AttachmentScanVerdict.Reject,
                StateVersion = 1,
                SizeBytes = 100,
                Reason = "malware"
            });

        Assert.True(result.Succeeded);
        Assert.Equal((short)AttachmentStatus.Rejected, result.Status);
        Assert.Equal(AttachmentStatus.Rejected, store.Current!.Status);
    }

    [Fact]
    public async Task HeadSizeMismatch_PassDegradesToRejected()
    {
        var store = new FakeAttachmentStore(Uploaded("a1", 100, "image/png", 1));
        // 对象实际大小 200 ≠ 票证 100 → 拒绝。
        var storage = new FakeObjectStorage(new ObjectHead("k1", 200, "hash", "image/png"));
        var processor = new AttachmentScanProcessor(
            store,
            NullLogger<AttachmentScanProcessor>.Instance,
            storage);

        var result = await processor.ProcessAsync(
            new AttachmentScanCommand
            {
                RequestId = "req-3",
                AttachmentId = "a1",
                Verdict = AttachmentScanVerdict.Pass,
                StateVersion = 1,
                SizeBytes = 100
            });

        Assert.True(result.Succeeded);
        Assert.Equal((short)AttachmentStatus.Rejected, result.Status);
    }

    [Fact]
    public async Task HeadContentTypeMismatch_PassDegradesToRejected()
    {
        var store = new FakeAttachmentStore(Uploaded("a1", 100, "image/png", 1));
        var storage = new FakeObjectStorage(new ObjectHead("k1", 100, "hash", "text/html"));
        var processor = new AttachmentScanProcessor(
            store,
            NullLogger<AttachmentScanProcessor>.Instance,
            storage);

        var result = await processor.ProcessAsync(
            new AttachmentScanCommand
            {
                RequestId = "req-4",
                AttachmentId = "a1",
                Verdict = AttachmentScanVerdict.Pass,
                StateVersion = 1,
                SizeBytes = 100
            });

        Assert.True(result.Succeeded);
        Assert.Equal((short)AttachmentStatus.Rejected, result.Status);
    }

    [Fact]
    public async Task StaleStateVersion_ReturnsFailure()
    {
        // 当前版本 2，扫描命令携带版本 1 → BeginScan 版本不匹配。
        var store = new FakeAttachmentStore(Uploaded("a1", 100, "image/png", 2));
        var processor = new AttachmentScanProcessor(store, NullLogger<AttachmentScanProcessor>.Instance);

        var result = await processor.ProcessAsync(
            new AttachmentScanCommand
            {
                RequestId = "req-5",
                AttachmentId = "a1",
                Verdict = AttachmentScanVerdict.Pass,
                StateVersion = 1,
                SizeBytes = 100
            });

        Assert.False(result.Succeeded);
        Assert.Equal("scan_begin_failed", result.ErrorCode);
    }

    private static RealtimeAttachmentRecord Uploaded(string id, long sizeBytes, string contentType, long stateVersion) =>
        new()
        {
            AttachmentId = id,
            UploaderUserId = 10,
            ObjectKey = "k1",
            ContentType = contentType,
            SizeBytes = sizeBytes,
            Status = AttachmentStatus.Uploaded,
            CreatedAtMs = 1,
            StateVersion = stateVersion
        };

    private sealed class FakeObjectStorage : IObjectStorage
    {
        private readonly ObjectHead? _head;

        public FakeObjectStorage(ObjectHead? head) => _head = head;

        public Task<ObjectHead?> HeadAsync(string objectKey, CancellationToken ct = default) =>
            Task.FromResult(_head);

        public Task DeleteAsync(string objectKey, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<string> CreateSignedDownloadUrlAsync(string objectKey, TimeSpan ttl, CancellationToken ct = default) =>
            Task.FromResult("https://example.local/" + objectKey);
    }

    /// <summary>最小化存储桩：模拟 BeginScan/CompleteScan 状态转换与版本递增。</summary>
    private sealed class FakeAttachmentStore : IRealtimeAttachmentStore
    {
        private RealtimeAttachmentRecord? _current;

        public FakeAttachmentStore(RealtimeAttachmentRecord initial) => _current = initial;

        public RealtimeAttachmentRecord? Current => _current;

        public Task<AttachmentScanTransitionResult> BeginScanAsync(
            string attachmentId,
            long expectedStateVersion,
            CancellationToken ct = default)
        {
            if (_current is null || _current.Status != AttachmentStatus.Uploaded
                || _current.StateVersion != expectedStateVersion)
            {
                return Task.FromResult(
                    AttachmentScanTransitionResult.Fail("scan_begin_failed", "状态或版本不匹配"));
            }
            _current = With(_current, AttachmentStatus.Scanning, _current.StateVersion + 1);
            return Task.FromResult(AttachmentScanTransitionResult.Ok(_current));
        }

        public Task<AttachmentScanTransitionResult> CompleteScanAsync(
            string attachmentId,
            long expectedStateVersion,
            AttachmentScanVerdict verdict,
            long sizeBytes,
            string? contentHash,
            string? contentType,
            string? reason,
            CancellationToken ct = default)
        {
            if (_current is null || _current.Status != AttachmentStatus.Scanning
                || _current.StateVersion != expectedStateVersion)
            {
                return Task.FromResult(
                    AttachmentScanTransitionResult.Fail("stale_state_version", "版本不匹配"));
            }
            var target = verdict == AttachmentScanVerdict.Pass
                ? AttachmentStatus.Available
                : AttachmentStatus.Rejected;
            _current = With(_current, target, _current.StateVersion + 1);
            return Task.FromResult(AttachmentScanTransitionResult.Ok(_current));
        }

        public Task<bool> MarkExpiredAsync(string attachmentId, long expectedStateVersion, CancellationToken ct = default)
        {
            if (_current is null || _current.StateVersion != expectedStateVersion)
                return Task.FromResult(false);
            _current = With(_current, AttachmentStatus.Expired, _current.StateVersion + 1);
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListExpiryCandidatesAsync(
            long cutoffMs, int take, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>(_current is null ? [] : [_current]);

        public Task<RealtimeAttachmentRecord> InsertConfirmedAsync(RealtimeAttachmentRecord attachment, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<int> BindToMessageAsync(string messageId, string? conversationId, long uploaderUserId, IReadOnlyList<string> attachmentIds, CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(long actorUserId, string attachmentId, long sizeBytes, string? contentHash, CancellationToken ct = default) =>
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

        private static RealtimeAttachmentRecord With(
            RealtimeAttachmentRecord r,
            AttachmentStatus status,
            long version) => new()
        {
            AttachmentId = r.AttachmentId,
            UploaderUserId = r.UploaderUserId,
            ObjectKey = r.ObjectKey,
            PublicUrl = r.PublicUrl,
            ContentType = r.ContentType,
            SizeBytes = r.SizeBytes,
            OriginalName = r.OriginalName,
            Status = status,
            MessageId = r.MessageId,
            ConversationId = r.ConversationId,
            ClientAttachmentId = r.ClientAttachmentId,
            CreatedAtMs = r.CreatedAtMs,
            ConfirmedAtMs = r.ConfirmedAtMs,
            BoundAtMs = r.BoundAtMs,
            ContentHash = r.ContentHash,
            StateVersion = version
        };
    }
}
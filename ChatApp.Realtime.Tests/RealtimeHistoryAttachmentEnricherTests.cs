using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeHistoryAttachmentEnricherTests
{
    [Fact]
    public async Task EnrichAsync_BatchesByMessageIds_NoNPlusOne()
    {
        var store = new CapturingAttachmentStore(
        [
            Record("a1", "m1", "photo.png"),
            Record("a2", "m1", "note.txt"),
            Record("a3", "m2", "other.bin")
        ]);

        var messages = new RealtimeHistoryMessage[]
        {
            History("m1", "hello"),
            History("m2", "world"),
            History("m3", "no-att")
        };

        var enriched = await RealtimeHistoryAttachmentEnricher.EnrichAsync(store, messages);

        Assert.Equal(1, store.ListCallCount);
        Assert.Equal(["m1", "m2", "m3"], store.LastMessageIds);

        Assert.Equal(2, enriched[0].Attachments!.Count);
        Assert.Contains(enriched[0].Attachments!, a => a.AttachmentId == "a1" && a.FileName == "photo.png");
        Assert.Contains(enriched[0].Attachments!, a => a.AttachmentId == "a2");
        Assert.Equal(AttachmentWireStatus.Available, enriched[0].Attachments![0].Status);

        Assert.Single(enriched[1].Attachments!);
        Assert.Equal("a3", enriched[1].Attachments![0].AttachmentId);

        Assert.Null(enriched[2].Attachments);
    }

    [Fact]
    public async Task EnrichAsync_EmptyMessages_DoesNotQueryStore()
    {
        var store = new CapturingAttachmentStore([]);
        var enriched = await RealtimeHistoryAttachmentEnricher.EnrichAsync(
            store,
            Array.Empty<RealtimeHistoryMessage>());

        Assert.Empty(enriched);
        Assert.Equal(0, store.ListCallCount);
    }

    [Fact]
    public async Task EnrichAsync_VoiceRegistryRows_CarryVoiceFieldsIntoHistory()
    {
        // VOICE-MSG-2：注册表语音行经回查后，历史消息附件必须携带语音 6 字段
        // （注册表由绑定链路以发送方元数据快照持久化语音列）。
        var store = new CapturingAttachmentStore(
        [
            VoiceRecord("att-voice-1", "m1"),
            Record("att-file-1", "m1", "photo.png")
        ]);

        var messages = new RealtimeHistoryMessage[] { History("m1", "voice") };
        var enriched = await RealtimeHistoryAttachmentEnricher.EnrichAsync(store, messages);

        var attachments = enriched[0].Attachments!;
        Assert.Equal(2, attachments.Count);

        var voice = attachments.Single(a => a.AttachmentId == "att-voice-1");
        Assert.True(voice.IsVoice);
        Assert.Equal("opus", voice.VoiceCodec);
        Assert.Equal("ogg", voice.VoiceContainer);
        Assert.Equal(3_200L, voice.VoiceDurationMs);
        Assert.Equal(48_000, voice.VoiceSampleRateHz);
        Assert.Equal((short)1, voice.VoiceChannels);
        // VOICE-MSG-2 waveform：注册表波形列经回查随 AttachmentRef 带出到历史消息。
        Assert.Equal(new byte[] { 8, 64, 255, 32 }, voice.VoiceWaveformPeaks);

        var file = attachments.Single(a => a.AttachmentId == "att-file-1");
        Assert.False(file.IsVoice);
        Assert.Null(file.VoiceCodec);
        Assert.Null(file.VoiceDurationMs);
        Assert.Null(file.VoiceWaveformPeaks);
    }

    private static RealtimeAttachmentRecord VoiceRecord(string id, string messageId) => new()
    {
        AttachmentId = id,
        UploaderUserId = 1,
        ObjectKey = $"k/{id}",
        ContentType = "audio/ogg",
        SizeBytes = 12_345,
        OriginalName = "voice.ogg",
        Status = AttachmentStatus.Bound,
        MessageId = messageId,
        ConversationId = "dm:1:2",
        CreatedAtMs = 1,
        BoundAtMs = 2,
        IsVoice = true,
        VoiceCodec = "opus",
        VoiceContainer = "ogg",
        VoiceDurationMs = 3_200,
        VoiceSampleRateHz = 48_000,
        VoiceChannels = 1,
        VoiceWaveformPeaks = [8, 64, 255, 32]
    };

    private static RealtimeHistoryMessage History(string messageId, string content) => new()
    {
        MessageId = messageId,
        ClientMessageId = $"c-{messageId}",
        SenderUserId = 1,
        ReceiverUserId = 2,
        ConversationId = "dm:1:2",
        Content = content,
        ReceivedAtMs = 100
    };

    private static RealtimeAttachmentRecord Record(string id, string messageId, string name) => new()
    {
        AttachmentId = id,
        UploaderUserId = 1,
        ObjectKey = $"k/{id}",
        ContentType = "application/octet-stream",
        SizeBytes = 10,
        OriginalName = name,
        Status = AttachmentStatus.Bound,
        MessageId = messageId,
        ConversationId = "dm:1:2",
        CreatedAtMs = 1,
        BoundAtMs = 2
    };

    private sealed class CapturingAttachmentStore(IReadOnlyList<RealtimeAttachmentRecord> rows)
        : IRealtimeAttachmentStore
    {
        public int ListCallCount { get; private set; }
        public IReadOnlyList<string>? LastMessageIds { get; private set; }

        public Task<RealtimeAttachmentRecord> InsertConfirmedAsync(
            RealtimeAttachmentRecord attachment,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(
            long actorUserId,
            string attachmentId,
            long sizeBytes,
            string? contentHash,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<int> BindToMessageAsync(
            string messageId,
            string? conversationId,
            long uploaderUserId,
            IReadOnlyList<string> attachmentIds,
            IReadOnlyList<ChatApp.Realtime.Abstractions.Messaging.AttachmentRef>? attachmentMetadata = null,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(
            IReadOnlyList<string> messageIds,
            CancellationToken ct = default)
        {
            ListCallCount++;
            LastMessageIds = messageIds;
            var set = messageIds.ToHashSet(StringComparer.Ordinal);
            return Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>(
                rows.Where(r => r.MessageId is not null && set.Contains(r.MessageId)).ToArray());
        }

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
            Task.FromResult<IReadOnlyList<string>>([]);

        public Task<IReadOnlyList<string>> DeleteByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public Task<int> DeleteByAttachmentIdsAsync(
            IReadOnlyList<string> attachmentIds,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentScanTransitionResult> BeginScanAsync(
            string attachmentId,
            long expectedStateVersion,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<AttachmentScanTransitionResult> CompleteScanAsync(
            string attachmentId,
            long expectedStateVersion,
            AttachmentScanVerdict verdict,
            long sizeBytes,
            string? contentHash,
            string? contentType,
            string? reason,
            CancellationToken ct = default,
            bool isVoice = false,
            string? voiceCodec = null,
            string? voiceContainer = null,
            long? voiceDurationMs = null,
            int? voiceSampleRateHz = null,
            short? voiceChannels = null) =>
            throw new NotSupportedException();

        public Task<bool> MarkExpiredAsync(
            string attachmentId,
            long expectedStateVersion,
            CancellationToken ct = default) =>
            Task.FromResult(false);

        public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListExpiryCandidatesAsync(
            long cutoffMs,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>([]);
    }
}

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

        public Task<int> BindToMessageAsync(
            string messageId,
            string? conversationId,
            long uploaderUserId,
            IReadOnlyList<string> attachmentIds,
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
    }
}

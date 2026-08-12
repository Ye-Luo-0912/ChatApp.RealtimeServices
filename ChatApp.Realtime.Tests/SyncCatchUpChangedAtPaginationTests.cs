using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Core.Sync;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1：通过真实 <see cref="DefaultSyncBootstrapQueryProcessor"/> 的 <c>PackCatchUp</c> 与
/// changed_at keyset 分页路径，验证编辑/撤回/Reaction 对变更水位（ChangedAtMs）的推进、
/// 空页但 HasMore 的打包语义，以及重复游标的稳定性。
/// </summary>
public sealed class SyncCatchUpChangedAtPaginationTests
{
    private const string ConversationId = "dm:42:43";

    [Fact]
    public async Task ProcessAsync_EditedTip_AdvancesNextCursorByChangedAtNotReceivedAt()
    {
        // msg-2 在 received=200 之后被编辑到 changed=500，且不是 tip（msg-3 更晚）。
        // PackCatchUp 截取 [msg-1, msg-2] 后 HasMore=true，NextCursor 必须用 msg-2 的 ChangedAtMs。
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-1", receivedAtMs: 100, changedAtMs: 100),
                Message("msg-2", receivedAtMs: 200, changedAtMs: 500, editVersion: 2, editedAtMs: 500),
                Message("msg-3", receivedAtMs: 700, changedAtMs: 700)
            ]);

        var page = await ProcessAsync(history, historyLimit: 2);

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal(["msg-1", "msg-2"], catchUp.Items.Select(m => m.MessageId).ToArray());
        Assert.True(catchUp.HasMore);
        Assert.NotNull(catchUp.NextCursor);
        // P1：变更水位必须取 ChangedAtMs（编辑后的时间），而非 ReceivedAtMs。
        Assert.Equal(500, catchUp.NextCursor.ChangedAtMs);
        Assert.Equal(200, catchUp.NextCursor.ReceivedAtMs);
        Assert.Equal("msg-2", catchUp.NextCursor.MessageId);
    }

    [Fact]
    public async Task ProcessAsync_RecalledMessage_AdvancesChangedAtWatermark()
    {
        // 撤回推进 changed_at（changed=600 > received=300），NextCursor 使用撤回变更水位。
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-1", receivedAtMs: 100, changedAtMs: 100),
                Message("msg-2", receivedAtMs: 300, changedAtMs: 600, recalledAtMs: 600),
                Message("msg-3", receivedAtMs: 800, changedAtMs: 800)
            ]);

        var page = await ProcessAsync(history, historyLimit: 2);

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal(["msg-1", "msg-2"], catchUp.Items.Select(m => m.MessageId).ToArray());
        Assert.True(catchUp.HasMore);
        Assert.Equal(600, catchUp.NextCursor?.ChangedAtMs);
        Assert.Equal(300, catchUp.NextCursor?.ReceivedAtMs);
    }

    [Fact]
    public async Task ProcessAsync_Reaction_AdvancesChangedAtWatermark()
    {
        // Reaction 推进 changed_at（changed=450 > received=250），NextCursor 使用该变更水位。
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-1", receivedAtMs: 100, changedAtMs: 100),
                Message("msg-2", receivedAtMs: 250, changedAtMs: 450, reactions: [new MessageReactionSummary { Emoji = "👍", Count = 1, ReactedByMe = true }]),
                Message("msg-3", receivedAtMs: 900, changedAtMs: 900)
            ]);

        var page = await ProcessAsync(history, historyLimit: 2);

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Equal(2, catchUp.Items.Count);
        Assert.True(catchUp.HasMore);
        Assert.Equal(450, catchUp.NextCursor?.ChangedAtMs);
        Assert.Equal(250, catchUp.NextCursor?.ReceivedAtMs);
    }

    [Fact]
    public async Task ProcessAsync_Mention_DoesNotAdvanceChangedAtWithoutMutation()
    {
        // 仅提及不推进 changed_at（changed=200 == 无编辑），NextCursor 回退到 ReceivedAtMs。
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-1", receivedAtMs: 100, changedAtMs: 100),
                Message("msg-2", receivedAtMs: 200, changedAtMs: 200, mentionedUserIds: [1, 2]),
                Message("msg-3", receivedAtMs: 900, changedAtMs: 900)
            ]);

        var page = await ProcessAsync(history, historyLimit: 2);

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Single(catchUp.Items, m => m.MessageId == "msg-2" && m.MentionedUserIds is [1, 2]);
        Assert.True(catchUp.HasMore);
        // mentioned msg-2 无 changed 变更 → 水位取 ReceivedAtMs。
        Assert.Equal(200, catchUp.NextCursor?.ChangedAtMs);
        Assert.Equal(200, catchUp.NextCursor?.ReceivedAtMs);
    }

    [Fact]
    public async Task ProcessAsync_SingleOverBudgetMessage_ReturnsEmptyPageButHasMore()
    {
        // 单条消息估算字节超过 PackingBudgetBytes：PackCatchUp 返回空 Items + HasMore=true + NextCursor=null，
        // 让客户端用原水位继续拉取，而不是丢消息。
        var bigContent = new string('x', 70_000);
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-huge", receivedAtMs: 100, changedAtMs: 100, content: bigContent)
            ]);

        var page = await ProcessAsync(history, historyLimit: 5);

        Assert.True(page.Succeeded);
        var catchUp = Assert.Single(page.CatchUps);
        Assert.Empty(catchUp.Items);
        Assert.True(catchUp.HasMore);
        Assert.Null(catchUp.NextCursor);
    }

    [Fact]
    public async Task ProcessAsync_DuplicateCursor_ReturnsStableNextCursor()
    {
        // 同一水位重复 bootstrap：changed_at keyset 确定性，两次返回相同集合与相同 NextCursor，不漂移。
        var history = new ChangedAtHistoryStore(
            ConversationId,
            [
                Message("msg-1", receivedAtMs: 100, changedAtMs: 100),
                Message("msg-2", receivedAtMs: 200, changedAtMs: 200),
                Message("msg-3", receivedAtMs: 300, changedAtMs: 300),
                Message("msg-4", receivedAtMs: 400, changedAtMs: 400)
            ]);

        var first = await ProcessAsync(history, historyLimit: 2);
        var second = await ProcessAsync(history, historyLimit: 2);

        var c1 = first.CatchUps.Single();
        var c2 = second.CatchUps.Single();
        Assert.Equal(c1.Items.Select(m => m.MessageId), c2.Items.Select(m => m.MessageId));
        Assert.Equal(c1.HasMore, c2.HasMore);
        Assert.Equal(c1.NextCursor!.ChangedAtMs, c2.NextCursor!.ChangedAtMs);
        Assert.Equal(c1.NextCursor!.MessageId, c2.NextCursor!.MessageId);
    }

    private static async Task<SyncBootstrapPage> ProcessAsync(
        ChangedAtHistoryStore history,
        int historyLimit)
    {
        var processor = new DefaultSyncBootstrapQueryProcessor(
            new SingleConversationStore(),
            history,
            new NoopRealtimeDeviceSyncCursorStore(),
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance),
            new NoopRealtimeReactionStore(NullLogger<NoopRealtimeReactionStore>.Instance));

        return await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "sync-p1",
            UserId = 42,
            ListLimit = 10,
            HistoryLimitPerConversation = historyLimit,
            MaxConversationsWithHistory = 5
        });
    }

    private static RealtimeHistoryMessage Message(
        string id,
        long receivedAtMs,
        long changedAtMs,
        string content = "hi",
        int editVersion = 1,
        long? editedAtMs = null,
        long? recalledAtMs = null,
        IReadOnlyList<MessageReactionSummary>? reactions = null,
        IReadOnlyList<long>? mentionedUserIds = null) =>
        new()
        {
            MessageId = id,
            ClientMessageId = $"client-{id}",
            SenderUserId = 43,
            ReceiverUserId = 42,
            ConversationId = ConversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs,
            ChangedAtMs = changedAtMs,
            EditVersion = editVersion,
            EditedAtMs = editedAtMs,
            RecalledAtMs = recalledAtMs,
            Reactions = reactions,
            MentionedUserIds = mentionedUserIds
        };

    private sealed class SingleConversationStore : IRealtimeConversationStore
    {
        public Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
            long userId,
            bool? beforeIsPinned,
            long? beforePinnedAtMs,
            long? beforeLastMessageAtMs,
            string? beforeConversationId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<ConversationListItem>>(
            [
                new ConversationListItem
                {
                    ConversationId = ConversationId,
                    UnreadCount = 0,
                    LastMessageId = "z-last",
                    LastMessageAtMs = 900
                }
            ]);

        public Task<IReadOnlyList<ConversationListItem>> QueryArchivedListAsync(
            long userId,
            bool? beforeIsPinned,
            long? beforePinnedAtMs,
            long? beforeLastMessageAtMs,
            string? beforeConversationId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<ConversationListItem>>([]);

        public Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
            long userId,
            string conversationId,
            long? readAtMs,
            string? readMessageId,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
            long userId,
            string conversationId,
            bool? pinned,
            bool? muted,
            long? mutedUntilMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();
    }

    /// <summary>
    /// 符合真实 Npgsql store 语义的 catch-up 双射：按 changed_at_ms 排序与过滤，
    /// 与 <c>PackCatchUp</c> 的变更水位推进相互印证。
    /// </summary>
    private sealed class ChangedAtHistoryStore : IRealtimeMessageHistoryStore
    {
        private readonly string _conversationId;
        private readonly IReadOnlyList<RealtimeHistoryMessage> _messages;

        public ChangedAtHistoryStore(
            string conversationId,
            IReadOnlyList<RealtimeHistoryMessage> messages)
        {
            _conversationId = conversationId;
            _messages = messages;
        }

        private IReadOnlyList<RealtimeHistoryMessage> Ordered =>
            _messages
                .OrderBy(m => m.ChangedAtMs)
                .ThenBy(m => m.MessageId, StringComparer.Ordinal)
                .ToArray();

        public Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
            long userId,
            IReadOnlyList<HistoryCatchUpQuery> queries,
            CancellationToken ct = default)
        {
            var map = new Dictionary<string, IReadOnlyList<RealtimeHistoryMessage>>(StringComparer.Ordinal);
            foreach (var q in queries)
            {
                IReadOnlyList<RealtimeHistoryMessage> page;
                if (q.AfterChangedAtMs is long afterAt
                    && !string.IsNullOrWhiteSpace(q.AfterMessageId))
                {
                    page = Ordered
                        .Where(m =>
                            m.ChangedAtMs > afterAt
                            || (m.ChangedAtMs == afterAt
                                && string.CompareOrdinal(m.MessageId, q.AfterMessageId) > 0))
                        .Take(q.Take)
                        .ToArray();
                }
                else
                {
                    page = Ordered.Take(q.Take).ToArray();
                }

                map[q.ConversationId] = page;
            }

            return Task.FromResult<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>>(map);
        }

        public Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
            IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
            CancellationToken ct = default)
        {
            var map = new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal);
            var tips = _messages
                .GroupBy(m => m.ConversationId)
                .ToDictionary(
                    g => g.Key!,
                    g => g.OrderByDescending(m => m.ChangedAtMs).First(),
                    StringComparer.Ordinal);
            foreach (var item in watermarks)
            {
                var tip = tips.GetValueOrDefault(item.ConversationId);
                var matched = Ordered.FirstOrDefault(m =>
                    string.Equals(m.ConversationId, item.ConversationId, StringComparison.Ordinal)
                    && string.Equals(m.MessageId, item.AfterMessageId, StringComparison.Ordinal));
                if (matched is not null
                    && tip is not null
                    && matched.ChangedAtMs <= tip.ChangedAtMs)
                {
                    map[item.ConversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = item.ConversationId,
                        AfterChangedAtMs = matched.ChangedAtMs,
                        AfterMessageId = matched.MessageId,
                        IsValid = true,
                        TipChangedAtMs = tip.ChangedAtMs,
                        TipMessageId = tip.MessageId,
                        ClientAfterChangedAtMs = item.AfterChangedAtMs,
                        ClientAfterMessageId = item.AfterMessageId
                    };
                }
                else
                {
                    map[item.ConversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = item.ConversationId,
                        AfterChangedAtMs = tip?.ChangedAtMs ?? 0,
                        AfterMessageId = tip?.MessageId ?? string.Empty,
                        IsValid = false,
                        InvalidationKind = matched is null
                            ? SyncWatermarkInvalidationKind.MessageNotFound
                            : SyncWatermarkInvalidationKind.AheadOfTip,
                        TipChangedAtMs = tip?.ChangedAtMs,
                        TipMessageId = tip?.MessageId,
                        ClientAfterChangedAtMs = item.AfterChangedAtMs,
                        ClientAfterMessageId = item.AfterMessageId
                    };
                }
            }

            return Task.FromResult<IReadOnlyDictionary<string, ResolvedSyncWatermark>>(map);
        }

        public Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
            long userId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RealtimeHistoryMessage>>(Ordered.Take(take).ToArray());

        public Task<ConversationMessageHistoryResult> QueryByConversationAsync(
            long userId,
            string conversationId,
            long? beforeReceivedAtMs,
            string? beforeMessageId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult(ConversationMessageHistoryResult.Ok(Ordered.Take(take).ToArray()));

        public Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
            long userId,
            string conversationId,
            long afterChangedAtMs,
            string afterMessageId,
            int take,
            CancellationToken ct = default) =>
            Task.FromResult(ConversationMessageHistoryResult.Ok(
                Ordered
                    .Where(m =>
                        m.ChangedAtMs > afterChangedAtMs
                        || (m.ChangedAtMs == afterChangedAtMs
                            && string.CompareOrdinal(m.MessageId, afterMessageId) > 0))
                    .Take(take)
                    .ToArray()));

        public Task<bool> IsConversationMemberAsync(
            long userId,
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult(true);

        public Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
            long userId,
            IReadOnlyCollection<string> conversationIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlySet<string>>(
                conversationIds.ToHashSet(StringComparer.Ordinal));

        public Task<RealtimeHistoryMessage?> TryGetByIdAsync(
            long userId,
            string messageId,
            CancellationToken ct = default) =>
            Task.FromResult(_messages.FirstOrDefault(m => m.MessageId == messageId));

        public Task<bool> CanAccessMessageAsync(
            long userId,
            string messageId,
            CancellationToken ct = default) =>
            Task.FromResult(_messages.Any(m => m.MessageId == messageId));
    }
}
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Infrastructure.Core.Sync;

public sealed class DefaultSyncBootstrapQueryProcessor : ISyncBootstrapQueryProcessor
{
    public const int MaximumListLimit = 100;
    public const int MaximumHistoryPerConversation = 50;
    public const int MaximumConversationsWithHistory = 20;
    public const int MaximumWatermarks = 50;
    public const int MaximumResponseBytes = RealtimeWireLimits.MaximumResponseBytes;

    private readonly IRealtimeConversationStore _conversationStore;
    private readonly IRealtimeMessageHistoryStore _historyStore;
    private readonly IRealtimeDeviceSyncCursorStore _deviceCursorStore;
    private readonly IRealtimeAttachmentStore _attachmentStore;
    private readonly IRealtimeReactionStore _reactionStore;
    private readonly IRelationshipStore? _relationshipStore;
    private readonly IRelationshipSyncCursorStore? _relationshipCursorStore;
    private readonly SyncBootstrapOptions _options;

    public DefaultSyncBootstrapQueryProcessor(
        IRealtimeConversationStore conversationStore,
        IRealtimeMessageHistoryStore historyStore,
        IRealtimeDeviceSyncCursorStore deviceCursorStore,
        IRealtimeAttachmentStore attachmentStore,
        IRealtimeReactionStore reactionStore,
        SyncBootstrapOptions? options = null,
        IRelationshipStore? relationshipStore = null,
        IRelationshipSyncCursorStore? relationshipCursorStore = null)
    {
        _conversationStore = conversationStore;
        _historyStore = historyStore;
        _deviceCursorStore = deviceCursorStore;
        _attachmentStore = attachmentStore;
        _reactionStore = reactionStore;
        _relationshipStore = relationshipStore;
        _relationshipCursorStore = relationshipCursorStore;
        _options = options ?? new SyncBootstrapOptions();
    }

    public async Task<SyncBootstrapPage> ProcessAsync(
        SyncBootstrapQuery query,
        CancellationToken ct = default)
    {
        var validationError = Validate(query);
        if (validationError is not null)
            return validationError;

        var listLimit = Math.Clamp(
            query.ListLimit == 0
                ? DefaultConversationListQueryProcessor.DefaultPageSize
                : query.ListLimit,
            1,
            MaximumListLimit);
        var historyLimit = Math.Clamp(
            query.HistoryLimitPerConversation == 0 ? 20 : query.HistoryLimitPerConversation,
            1,
            MaximumHistoryPerConversation);
        var maxCatchUps = Math.Clamp(
            query.MaxConversationsWithHistory == 0 ? 10 : query.MaxConversationsWithHistory,
            0,
            MaximumConversationsWithHistory);

        var listRows = await _conversationStore.QueryListAsync(
                query.UserId,
                beforeIsPinned: null,
                beforePinnedAtMs: null,
                beforeLastMessageAtMs: null,
                beforeConversationId: null,
                take: listLimit + 1,
                ct)
            .ConfigureAwait(false);

        var conversations = new List<ConversationListItem>(Math.Min(listLimit, listRows.Count));
        var responseBytes = 0;
        foreach (var row in listRows)
        {
            if (conversations.Count >= listLimit)
                break;

            var itemBytes = EstimateConversationBytes(row);
            if (conversations.Count > 0 && responseBytes + itemBytes > RealtimeWireLimits.PackingBudgetBytes)
                break;

            conversations.Add(row);
            responseBytes += itemBytes;
        }

        var conversationsHasMore = listRows.Count > conversations.Count;
        var conversationsNextCursor = conversationsHasMore && conversations.Count > 0
            ? new ConversationListCursor(
                conversations[^1].IsPinned,
                conversations[^1].PinnedAtMs,
                conversations[^1].LastMessageAtMs,
                conversations[^1].ConversationId)
            : null;

        var effectiveWatermarks = query.Watermarks;
        if ((effectiveWatermarks is null || effectiveWatermarks.Count == 0)
            && query.DeviceIdHash is ulong deviceIdHash)
        {
            var stored = await _deviceCursorStore
                .LoadAsync(query.UserId, deviceIdHash, MaximumWatermarks, ct)
                .ConfigureAwait(false);
            if (stored.Count > 0)
            {
                effectiveWatermarks = stored
                    .Select(static cursor => new ConversationSyncWatermark
                    {
                        ConversationId = cursor.ConversationId,
                        AfterChangedAtMs = cursor.AfterChangedAtMs,
                        AfterMessageId = cursor.AfterMessageId
                    })
                    .ToArray();
            }
        }

        var catchUps = new List<ConversationHistoryCatchUp>(maxCatchUps);
        Dictionary<string, ResolvedSyncWatermark>? incrementalWatermarks = null;
        IReadOnlyList<SyncCursorResetRequired> allResets = [];
        IReadOnlyList<SyncCursorResetRequired> resetsRequired = [];
        if (effectiveWatermarks is { Count: > 0 })
        {
            var resolution = await BuildWatermarkResolutionAsync(
                    query.UserId,
                    effectiveWatermarks,
                    conversations,
                    ct)
                .ConfigureAwait(false);
            incrementalWatermarks = resolution.Incremental;
            allResets = resolution.ResetsRequired;

            var (kept, resetBytes) = PackResetsForBudget(allResets, responseBytes);
            resetsRequired = kept;
            responseBytes += resetBytes;
        }

        if (maxCatchUps > 0 && responseBytes < RealtimeWireLimits.PackingBudgetBytes)
        {
            incrementalWatermarks ??= new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal);
            var resetIds = new HashSet<string>(
                allResets.Select(static r => r.ConversationId),
                StringComparer.Ordinal);

            var catchUpIds = SelectCatchUpConversationIds(
                    conversations,
                    incrementalWatermarks,
                    maxCatchUps)
                .Where(id => !resetIds.Contains(id))
                .ToArray();
            if (catchUpIds.Length > 0)
            {
                var catchUpQueries = catchUpIds
                    .Select(conversationId =>
                    {
                        if (incrementalWatermarks.TryGetValue(conversationId, out var watermark))
                        {
                            return new HistoryCatchUpQuery
                            {
                                ConversationId = conversationId,
                                AfterChangedAtMs = watermark.AfterChangedAtMs,
                                AfterMessageId = watermark.AfterMessageId,
                                Take = historyLimit + 1
                            };
                        }

                        return new HistoryCatchUpQuery
                        {
                            ConversationId = conversationId,
                            Take = historyLimit + 1
                        };
                    })
                    .ToArray();

                var batchRows = await _historyStore
                    .QueryCatchUpsAsync(query.UserId, catchUpQueries, ct)
                    .ConfigureAwait(false);

                // Batch-enrich attachments for ALL catch-up conversations in one call instead of
                // per-conversation N+1 round-trips to the attachment store.
                var flattened = batchRows.Count == 0
                    ? Array.Empty<RealtimeHistoryMessage>()
                    : batchRows.Values
                        .SelectMany(static rows => rows)
                        .ToArray();
                var enrichedFlat = await RealtimeHistoryAttachmentEnricher
                    .EnrichAsync(_attachmentStore, flattened, ct)
                    .ConfigureAwait(false);
                enrichedFlat = await RealtimeHistoryReactionEnricher
                    .EnrichAsync(_reactionStore, enrichedFlat, query.UserId, ct)
                    .ConfigureAwait(false);
                var enrichedByMessageId = new Dictionary<string, RealtimeHistoryMessage>(
                    enrichedFlat.Count,
                    StringComparer.Ordinal);
                foreach (var message in enrichedFlat)
                    enrichedByMessageId[message.MessageId] = message;

                foreach (var conversationId in catchUpIds)
                {
                    ct.ThrowIfCancellationRequested();
                    if (responseBytes >= RealtimeWireLimits.PackingBudgetBytes)
                        break;

                    if (!batchRows.TryGetValue(conversationId, out var rows))
                        rows = Array.Empty<RealtimeHistoryMessage>();

                    var enrichedRows = rows.Count == 0
                        ? rows
                        : rows
                            .Select(row => enrichedByMessageId.TryGetValue(row.MessageId, out var enriched)
                                ? enriched
                                : row)
                            .ToArray();

                    catchUps.Add(PackCatchUp(
                        conversationId,
                        enrichedRows,
                        historyLimit,
                        ref responseBytes));
                }
            }
        }

        if (query.DeviceIdHash is ulong resetDeviceId && allResets.Count > 0)
        {
            // Purge poisoned device cursors for reset conversations so the next bootstrap doesn't
            // keep re-loading a cursor that will only ever resolve to a reset.
            await _deviceCursorStore
                .DeleteAsync(
                    query.UserId,
                    resetDeviceId,
                    allResets.Select(static r => r.ConversationId).ToArray(),
                    ct)
                .ConfigureAwait(false);
        }

        // 关系列表增量同步：在 EnforceByteBudget 之前构造，使其纳入字节预算硬约束。
        // 与会话同步并行——RelationshipSyncWatermark 与 ConversationSyncWatermark 维度独立。
        var relationshipCatchUps = await BuildRelationshipCatchUpsAsync(query, listLimit, ct)
            .ConfigureAwait(false);

        // Perf-5：快速估算仅用于初始筛选；最终响应字节数必须通过实际 UTF-8 序列化硬约束。
        // 估算会漏算 Reply/Forward preview、MentionedUserIds/Roles、Reactions、Conversation title、
        // JSON 转义与包装结构，因此这里做最终实际序列化校验，超预算则按优先级逐项回退。
        var (finalPage, finalCatchUps, finalRelationshipCatchUps) = EnforceByteBudget(
            query.RequestId,
            conversations,
            conversationsNextCursor,
            conversationsHasMore,
            catchUps,
            resetsRequired,
            relationshipCatchUps);

        if (query.DeviceIdHash is ulong persistDeviceId)
        {
            // Only persist last returned catch-up messages — never raw/reset client watermarks.
            // Perf-5：持久化必须在裁剪之后，避免推进被裁剪掉的 tip。
            var toPersist = BuildDeviceCursorsToPersist(finalCatchUps);
            if (toPersist.Count > 0)
            {
                await _deviceCursorStore
                    .UpsertManyAsync(query.UserId, persistDeviceId, toPersist, ct)
                    .ConfigureAwait(false);
            }

            // 关系列表游标持久化：仅推进非 reset 且实际返回了条目的 catch-up 的新水位。
            // ResetRequired 的列表不推进水位（客户端应保留旧水位并按 reset 语义清空本地）。
            // BlockedUsers 无服务端水位（NewAfterChangedAtMs=0），不持久化。
            var relCursorsToPersist = BuildRelationshipCursorsToPersist(finalRelationshipCatchUps);
            if (relCursorsToPersist.Count > 0 && _relationshipCursorStore is not null)
            {
                await _relationshipCursorStore
                    .UpsertManyAsync(query.UserId, persistDeviceId, relCursorsToPersist, ct)
                    .ConfigureAwait(false);
            }
        }

        return finalPage;
    }

    /// <summary>
    /// 构造关系列表增量同步结果。
    /// <para>
    /// 水位来源优先级：
    /// 1) 客户端在 query.RelationshipWatermarks 中显式传入的水位；
    /// 2) 设备级持久化游标（query.DeviceIdHash 提供）。
    /// </para>
    /// <para>
    /// 服务端过滤：Friends / FriendRequests 在 SQL 中按 created_at_ms &gt; after 过滤；
    /// BlockedUsers 表无变更时间戳，返回全量列表，由客户端按本地缓存 diff。
    /// </para>
    /// <para>
    /// 新水位：取返回 items 中最大的 CreatedAtMs（无 items 时保留原水位）。
    /// BlockedUsers 的 NewAfterChangedAtMs 始终为 0（无法推进水位）。
    /// </para>
    /// </summary>
    private async Task<List<RelationshipCatchUp>> BuildRelationshipCatchUpsAsync(
        SyncBootstrapQuery query, int listLimit, CancellationToken ct)
    {
        if (_relationshipStore is null)
            return [];

        var relLimit = query.RelationshipListLimit is int rl && rl > 0
            ? Math.Clamp(rl, 1, MaximumListLimit)
            : listLimit;

        // 1) 解析 after-by-listType 映射
        var afterByListType = new Dictionary<byte, long>();
        var clientWatermarks = query.RelationshipWatermarks;
        if (clientWatermarks is { Count: > 0 })
        {
            foreach (var wm in clientWatermarks)
                afterByListType[(byte)wm.ListType] = wm.AfterChangedAtMs;
        }
        else if (query.DeviceIdHash is ulong did && _relationshipCursorStore is not null)
        {
            var stored = await _relationshipCursorStore
                .LoadAsync(query.UserId, did, ct)
                .ConfigureAwait(false);
            foreach (var cursor in stored)
                afterByListType[cursor.ListType] = cursor.AfterChangedAtMs;
        }

        // 2) 三种 list type 顺序：Friends / FriendRequests / BlockedUsers
        var result = new List<RelationshipCatchUp>(3);
        var listTypes = new[]
        {
            RelationshipListType.Friends,
            RelationshipListType.FriendRequests,
            RelationshipListType.BlockedUsers
        };

        foreach (var listType in listTypes)
        {
            ct.ThrowIfCancellationRequested();
            afterByListType.TryGetValue((byte)listType, out var afterMs);

            IReadOnlyList<RelationshipListItem> items;
            try
            {
                items = listType switch
                {
                    RelationshipListType.Friends => await _relationshipStore
                        .ListFriendsAsync(query.UserId, relLimit, cursor: null, afterMs, ct)
                        .ConfigureAwait(false),
                    RelationshipListType.FriendRequests => await _relationshipStore
                        .ListFriendRequestsAsync(query.UserId, relLimit, cursor: null, afterMs, ct)
                        .ConfigureAwait(false),
                    RelationshipListType.BlockedUsers => await _relationshipStore
                        .ListBlockedUsersAsync(query.UserId, relLimit, cursor: null, afterMs, ct)
                        .ConfigureAwait(false),
                    _ => Array.Empty<RelationshipListItem>()
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // 关系列表查询失败不应导致整个 SyncBootstrap 失败：降级为空 catch-up。
                // 客户端会保留旧水位，下次 bootstrap 再尝试。
                result.Add(new RelationshipCatchUp
                {
                    ListType = listType,
                    Items = [],
                    HasMore = false,
                    NextCursor = null,
                    NewAfterChangedAtMs = afterMs,
                    ResetRequired = false,
                    ResetReason = null
                });
                continue;
            }

            // 服务端已按 LIMIT relLimit+1 取数；超过 size 表示有更多
            var size = relLimit;
            var hasMore = items.Count > size;
            var page = hasMore ? items.Take(size).ToArray() : items;

            // 新水位：BlockedUsers 表无 created_at_ms（始终 0），不推进；
            // 其它列表取最大 CreatedAtMs（单调推进）
            long newAfterMs = afterMs;
            if (listType != RelationshipListType.BlockedUsers)
            {
                foreach (var item in page)
                {
                    if (item.CreatedAtMs > newAfterMs)
                        newAfterMs = item.CreatedAtMs;
                }
            }

            result.Add(new RelationshipCatchUp
            {
                ListType = listType,
                Items = page,
                HasMore = hasMore,
                NextCursor = null, // 当前实现不支持分页游标：单次返回 relLimit 条
                NewAfterChangedAtMs = newAfterMs,
                ResetRequired = false,
                ResetReason = null
            });
        }

        return result;
    }

    /// <summary>
    /// 仅持久化实际返回了条目且非 BlockedUsers（无水位语义）的 catch-up 新水位。
    /// </summary>
    private static IReadOnlyList<RelationshipSyncCursor> BuildRelationshipCursorsToPersist(
        IReadOnlyList<RelationshipCatchUp> catchUps)
    {
        var list = new List<RelationshipSyncCursor>(catchUps.Count);
        foreach (var catchUp in catchUps)
        {
            if (catchUp.ResetRequired)
                continue;
            if (catchUp.NewAfterChangedAtMs <= 0)
                continue;
            if (catchUp.Items.Count == 0)
                continue;
            // BlockedUsers 的 NewAfterChangedAtMs 始终 0，天然被过滤
            list.Add(new RelationshipSyncCursor
            {
                ListType = (byte)catchUp.ListType,
                AfterChangedAtMs = catchUp.NewAfterChangedAtMs,
                UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                LastSeenAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });
        }
        return list;
    }

    /// <summary>
    /// Perf-5：通过实际 UTF-8 JSON 序列化对响应字节数进行硬约束。
    /// 超过 <see cref="MaximumResponseBytes"/> 时按优先级逐项回退：
    /// 1) 先从末尾移除 catch-up 历史条目（最低优先级，客户端可后续拉取）；
    /// 2) 仍超预算则移除整个 catch-up 会话；
    /// 3) 最后才移除会话列表项（从末尾起）。
    /// Resets 始终保留到最后——它们指导客户端清空本地缓存，不能因字节预算丢失。
    /// </summary>
    private static (SyncBootstrapPage Page, List<ConversationHistoryCatchUp> FinalCatchUps, List<RelationshipCatchUp> FinalRelationshipCatchUps) EnforceByteBudget(
        string requestId,
        List<ConversationListItem> conversations,
        ConversationListCursor? conversationsNextCursor,
        bool conversationsHasMore,
        List<ConversationHistoryCatchUp> catchUps,
        IReadOnlyList<SyncCursorResetRequired> resetsRequired,
        List<RelationshipCatchUp> relationshipCatchUps)
    {
        var serverTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var page = SyncBootstrapPage.Success(
            requestId,
            serverTimeMs,
            conversations,
            conversationsNextCursor,
            conversationsHasMore,
            catchUps,
            resetsRequired,
            relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);

        var currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
        if (currentBytes <= MaximumResponseBytes)
            return (page, catchUps, relationshipCatchUps);

        // 阶段 1：逐条移除 catch-up 历史条目（从最后一个 catch-up 的尾部开始）。
        while (currentBytes > MaximumResponseBytes)
        {
            var trimmed = false;
            for (var i = catchUps.Count - 1; i >= 0 && currentBytes > MaximumResponseBytes; i--)
            {
                var catchUp = catchUps[i];
                if (catchUp.Items.Count == 0)
                    continue;

                var newItems = catchUp.Items.Take(catchUp.Items.Count - 1).ToArray();
                var last = newItems.Length == 0 ? null : newItems[^1];
                catchUps[i] = new ConversationHistoryCatchUp
                {
                    ConversationId = catchUp.ConversationId,
                    Items = newItems,
                    HasMore = true,
                    NextCursor = last is not null
                        ? new MessageHistoryCursor(
                            last.ChangedAtMs > 0 ? last.ChangedAtMs : last.ReceivedAtMs,
                            last.MessageId)
                        : null
                };
                trimmed = true;
                page = SyncBootstrapPage.Success(
                    requestId,
                    serverTimeMs,
                    conversations,
                    conversationsNextCursor,
                    conversationsHasMore,
                    catchUps,
                    resetsRequired,
                    relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);
                currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
            }

            if (!trimmed)
                break;
        }

        // 阶段 2：移除整个 catch-up 会话（从末尾起）。
        while (currentBytes > MaximumResponseBytes && catchUps.Count > 0)
        {
            catchUps.RemoveAt(catchUps.Count - 1);
            page = SyncBootstrapPage.Success(
                requestId,
                serverTimeMs,
                conversations,
                conversationsNextCursor,
                conversationsHasMore,
                catchUps,
                resetsRequired,
                relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);
            currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
        }

        // 阶段 2.5：若仍超预算，逐条移除关系 catch-up 条目（从最后一个 catch-up 尾部起）。
        // 关系列表条目优先级高于会话列表项（关系变更影响在线状态/会话可见性），
        // 但低于会话 catch-up（消息投递优先级最高）。
        while (currentBytes > MaximumResponseBytes)
        {
            var trimmed = false;
            for (var i = relationshipCatchUps.Count - 1; i >= 0 && currentBytes > MaximumResponseBytes; i--)
            {
                var relCatchUp = relationshipCatchUps[i];
                if (relCatchUp.Items.Count == 0)
                    continue;

                var newItems = relCatchUp.Items.Take(relCatchUp.Items.Count - 1).ToArray();
                // 重新计算 NewAfterChangedAtMs：仅保留实际返回条目的最大 CreatedAtMs
                long newAfterMs = 0;
                foreach (var item in newItems)
                {
                    if (item.CreatedAtMs > newAfterMs)
                        newAfterMs = item.CreatedAtMs;
                }
                relationshipCatchUps[i] = new RelationshipCatchUp
                {
                    ListType = relCatchUp.ListType,
                    Items = newItems,
                    HasMore = true,
                    NextCursor = relCatchUp.NextCursor,
                    NewAfterChangedAtMs = newAfterMs > 0 ? newAfterMs : relCatchUp.NewAfterChangedAtMs,
                    ResetRequired = relCatchUp.ResetRequired,
                    ResetReason = relCatchUp.ResetReason
                };
                trimmed = true;
                page = SyncBootstrapPage.Success(
                    requestId,
                    serverTimeMs,
                    conversations,
                    conversationsNextCursor,
                    conversationsHasMore,
                    catchUps,
                    resetsRequired,
                    relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);
                currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
            }

            if (!trimmed)
                break;
        }

        // 阶段 2.6：移除整个关系 catch-up（从末尾起）。
        while (currentBytes > MaximumResponseBytes && relationshipCatchUps.Count > 0)
        {
            relationshipCatchUps.RemoveAt(relationshipCatchUps.Count - 1);
            page = SyncBootstrapPage.Success(
                requestId,
                serverTimeMs,
                conversations,
                conversationsNextCursor,
                conversationsHasMore,
                catchUps,
                resetsRequired,
                relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);
            currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
        }

        // 阶段 3：移除会话列表项（从末尾起），同时重算游标与 hasMore。
        while (currentBytes > MaximumResponseBytes && conversations.Count > 0)
        {
            conversations.RemoveAt(conversations.Count - 1);
            conversationsHasMore = true;
            if (conversations.Count > 0)
            {
                var last = conversations[^1];
                conversationsNextCursor = new ConversationListCursor(
                    last.IsPinned,
                    last.PinnedAtMs,
                    last.LastMessageAtMs,
                    last.ConversationId);
            }
            else
            {
                conversationsNextCursor = null;
            }

            page = SyncBootstrapPage.Success(
                requestId,
                serverTimeMs,
                conversations,
                conversationsNextCursor,
                conversationsHasMore,
                catchUps,
                resetsRequired,
                relationshipCatchUps.Count == 0 ? null : relationshipCatchUps);
            currentBytes = MeasureSyncBootstrapPageUtf8Json(page);
        }

        return (page, catchUps, relationshipCatchUps);
    }

    /// <summary>
    /// Perf-4：直接序列化为 UTF-8 字节并返回长度，避免中间 string 分配。
    /// </summary>
    private static int MeasureSyncBootstrapPageUtf8Json(SyncBootstrapPage page) =>
        JsonSerializer.SerializeToUtf8Bytes(
            page,
            RealtimeJsonSerializerContext.Default.SyncBootstrapPage).Length;

    /// <summary>
    /// 仅持久化 bootstrap 实际返回的最后一条消息。
    /// 空 catch-up / ResetRequired 不得推进游标，
    /// 否则单调前进的设备游标会永久跳过未投递历史。
    /// </summary>
    private static IReadOnlyList<DeviceSyncCursor> BuildDeviceCursorsToPersist(
        IReadOnlyList<ConversationHistoryCatchUp> catchUps)
    {
        var map = new Dictionary<string, DeviceSyncCursor>(StringComparer.Ordinal);
        foreach (var catchUp in catchUps)
        {
            if (catchUp.Items.Count == 0)
                continue;

            var last = catchUp.Items[^1];
            map[catchUp.ConversationId] = new DeviceSyncCursor
            {
                ConversationId = catchUp.ConversationId,
                AfterChangedAtMs = last.ChangedAtMs > 0 ? last.ChangedAtMs : last.ReceivedAtMs,
                AfterMessageId = last.MessageId
            };
        }

        return map.Values.ToArray();
    }

    private async Task<(
            Dictionary<string, ResolvedSyncWatermark> Incremental,
            IReadOnlyList<SyncCursorResetRequired> ResetsRequired)>
        BuildWatermarkResolutionAsync(
            long userId,
            IReadOnlyList<ConversationSyncWatermark> watermarks,
            IReadOnlyList<ConversationListItem> conversations,
            CancellationToken ct)
    {
        var incremental = new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal);
        var resets = new Dictionary<string, SyncCursorResetRequired>(StringComparer.Ordinal);

        var candidates = new Dictionary<string, ConversationSyncWatermark>(StringComparer.Ordinal);
        foreach (var item in watermarks)
        {
            if (string.IsNullOrWhiteSpace(item.ConversationId)
                || string.IsNullOrWhiteSpace(item.AfterMessageId)
                || item.AfterChangedAtMs <= 0)
            {
                continue;
            }

            candidates[item.ConversationId.Trim()] = item;
        }

        if (candidates.Count == 0)
            return (incremental, []);

        var tipByConversation = conversations.ToDictionary(
            static c => c.ConversationId,
            static c => c,
            StringComparer.Ordinal);

        var knownMembers = new HashSet<string>(tipByConversation.Keys, StringComparer.Ordinal);
        var unknownIds = candidates.Keys
            .Where(id => !knownMembers.Contains(id))
            .ToArray();
        var authorizedUnknown = unknownIds.Length == 0
            ? (IReadOnlySet<string>)new HashSet<string>(StringComparer.Ordinal)
            : await _historyStore
                .FilterMemberConversationIdsAsync(userId, unknownIds, ct)
                .ConfigureAwait(false);

        var resolveInputs = new List<ConversationSyncWatermarkInput>(candidates.Count);
        foreach (var (conversationId, watermark) in candidates)
        {
            if (!knownMembers.Contains(conversationId) && !authorizedUnknown.Contains(conversationId))
            {
                resets[conversationId] = new SyncCursorResetRequired
                {
                    ConversationId = conversationId,
                    Reason = SyncCursorResetReason.MembershipLost,
                    TipMessageId = null,
                    TipChangedAtMs = null,
                    ClientAfterChangedAtMs = watermark.AfterChangedAtMs,
                    ClientAfterMessageId = watermark.AfterMessageId
                };
                continue;
            }

            tipByConversation.TryGetValue(conversationId, out var listItem);
            resolveInputs.Add(new ConversationSyncWatermarkInput
            {
                ConversationId = conversationId,
                AfterChangedAtMs = watermark.AfterChangedAtMs,
                AfterMessageId = watermark.AfterMessageId,
                TipChangedAtMs = listItem?.LastMessageAtMs,
                TipMessageId = listItem?.LastMessageId
            });
        }

        if (resolveInputs.Count > 0)
        {
            var resolved = await _historyStore
                .ResolveSyncWatermarksAsync(resolveInputs, ct)
                .ConfigureAwait(false);

            var maxGapMs = _options.MaxCatchUpGapMs > 0
                ? _options.MaxCatchUpGapMs
                : (long?)null;
            var retentionHorizonMs = _options.RetentionHorizonMs > 0
                ? _options.RetentionHorizonMs
                : (long?)null;

            foreach (var (conversationId, watermark) in resolved)
            {
                if (!watermark.IsValid)
                {
                    var reason = MapInvalidation(watermark.InvalidationKind);
                    // 随机/已删消息 vs 已过期历史：无效游标若明显早于 tip−retention，标为 BeyondRetention。
                    if (reason == SyncCursorResetReason.MessageNotFound
                        && retentionHorizonMs is long retentionForInvalid
                        && (watermark.TipChangedAtMs ?? tipByConversation.GetValueOrDefault(conversationId)?.LastMessageAtMs)
                            is long tipForInvalid
                        && (watermark.ClientAfterChangedAtMs > 0
                            ? watermark.ClientAfterChangedAtMs
                            : watermark.AfterChangedAtMs) is var clientForInvalid
                        && clientForInvalid > 0
                        && clientForInvalid < tipForInvalid - retentionForInvalid)
                    {
                        reason = SyncCursorResetReason.BeyondRetention;
                    }

                    resets[conversationId] = ToResetRequired(watermark, reason);
                    continue;
                }

                tipByConversation.TryGetValue(conversationId, out var listItem);
                var tipAt = watermark.TipChangedAtMs ?? listItem?.LastMessageAtMs;
                var clientAfter = watermark.ClientAfterChangedAtMs > 0
                    ? watermark.ClientAfterChangedAtMs
                    : watermark.AfterChangedAtMs;

                // Retention takes precedence over a plain gap: a cursor beyond the retention
                // horizon can never be served incrementally regardless of gap policy.
                if (retentionHorizonMs is long retention
                    && tipAt is long retentionTip
                    && clientAfter < retentionTip - retention)
                {
                    resets[conversationId] = new SyncCursorResetRequired
                    {
                        ConversationId = conversationId,
                        Reason = SyncCursorResetReason.BeyondRetention,
                        TipMessageId = watermark.TipMessageId ?? listItem?.LastMessageId,
                        TipChangedAtMs = tipAt,
                        ClientAfterChangedAtMs = clientAfter,
                        ClientAfterMessageId = string.IsNullOrWhiteSpace(watermark.ClientAfterMessageId)
                            ? null
                            : watermark.ClientAfterMessageId
                    };
                    continue;
                }

                if (maxGapMs is long gap
                    && tipAt is long tip
                    && tip - clientAfter > gap)
                {
                    resets[conversationId] = new SyncCursorResetRequired
                    {
                        ConversationId = conversationId,
                        Reason = SyncCursorResetReason.GapTooLarge,
                        TipMessageId = watermark.TipMessageId ?? listItem?.LastMessageId,
                        TipChangedAtMs = tipAt,
                        ClientAfterChangedAtMs = clientAfter,
                        ClientAfterMessageId = string.IsNullOrWhiteSpace(watermark.ClientAfterMessageId)
                            ? null
                            : watermark.ClientAfterMessageId
                    };
                    continue;
                }

                incremental[conversationId] = watermark;
            }
        }

        return (incremental, resets.Values.ToArray());
    }

    private static SyncCursorResetReason MapInvalidation(
        SyncWatermarkInvalidationKind? kind) =>
        kind switch
        {
            SyncWatermarkInvalidationKind.AheadOfTip => SyncCursorResetReason.AheadOfTip,
            SyncWatermarkInvalidationKind.BeyondRetention => SyncCursorResetReason.BeyondRetention,
            _ => SyncCursorResetReason.MessageNotFound
        };

    private static SyncCursorResetRequired ToResetRequired(
        ResolvedSyncWatermark watermark,
        SyncCursorResetReason reason)
    {
        var tipId = !string.IsNullOrWhiteSpace(watermark.TipMessageId)
            ? watermark.TipMessageId
            : (string.IsNullOrWhiteSpace(watermark.AfterMessageId)
                ? null
                : watermark.AfterMessageId);
        var tipAt = watermark.TipChangedAtMs
                    ?? (watermark.AfterChangedAtMs > 0 ? watermark.AfterChangedAtMs : null);
        return new SyncCursorResetRequired
        {
            ConversationId = watermark.ConversationId,
            Reason = reason,
            TipMessageId = tipId,
            TipChangedAtMs = tipAt,
            ClientAfterChangedAtMs = watermark.ClientAfterChangedAtMs > 0
                ? watermark.ClientAfterChangedAtMs
                : null,
            ClientAfterMessageId = string.IsNullOrWhiteSpace(watermark.ClientAfterMessageId)
                ? null
                : watermark.ClientAfterMessageId
        };
    }

    private static ConversationHistoryCatchUp PackCatchUp(
        string conversationId,
        IReadOnlyList<RealtimeHistoryMessage> rows,
        int historyLimit,
        ref int responseBytes)
    {
        var items = new List<RealtimeHistoryMessage>(Math.Min(historyLimit, rows.Count));
        foreach (var row in rows)
        {
            if (items.Count >= historyLimit)
                break;

            var itemBytes = EstimateHistoryBytes(row);
            if (items.Count > 0 && responseBytes + itemBytes > RealtimeWireLimits.PackingBudgetBytes)
                break;
            if (items.Count == 0 && responseBytes + itemBytes > RealtimeWireLimits.PackingBudgetBytes)
            {
                return new ConversationHistoryCatchUp
                {
                    ConversationId = conversationId,
                    Items = [],
                    HasMore = true,
                    NextCursor = null
                };
            }

            items.Add(row);
            responseBytes += itemBytes;
        }

        var hasMore = rows.Count > items.Count;
        var last = items.Count == 0 ? null : items[^1];
        return new ConversationHistoryCatchUp
        {
            ConversationId = conversationId,
            Items = items,
            HasMore = hasMore,
            NextCursor = hasMore && last is not null
                ? new MessageHistoryCursor(last.ChangedAtMs > 0 ? last.ChangedAtMs : last.ReceivedAtMs, last.MessageId)
                : null
        };
    }

    private static IEnumerable<string> SelectCatchUpConversationIds(
        IReadOnlyList<ConversationListItem> conversations,
        Dictionary<string, ResolvedSyncWatermark> watermarks,
        int maxCatchUps)
    {
        var selected = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in conversations)
        {
            if (item.UnreadCount > 0 && selected.Add(item.ConversationId) && selected.Count >= maxCatchUps)
                return selected;
        }

        foreach (var conversationId in watermarks.Keys)
        {
            if (selected.Add(conversationId) && selected.Count >= maxCatchUps)
                return selected;
        }

        foreach (var item in conversations)
        {
            if (selected.Add(item.ConversationId) && selected.Count >= maxCatchUps)
                return selected;
        }

        return selected;
    }

    private static SyncBootstrapPage? Validate(SyncBootstrapQuery query)
    {
        if (string.IsNullOrWhiteSpace(query.RequestId) || query.RequestId.Length > 64)
            return SyncBootstrapPage.Failed(
                query.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (query.UserId <= 0)
            return SyncBootstrapPage.Failed(query.RequestId, "invalid_user_id", "用户编号必须大于 0。");
        if (query.ListLimit < 0
            || query.HistoryLimitPerConversation < 0
            || query.MaxConversationsWithHistory < 0)
        {
            return SyncBootstrapPage.Failed(query.RequestId, "invalid_limit", "分页参数不能为负数。");
        }

        if (query.Watermarks is { Count: > MaximumWatermarks })
            return SyncBootstrapPage.Failed(
                query.RequestId,
                "invalid_watermarks",
                $"水位数量不能超过 {MaximumWatermarks}。");

        return null;
    }

    /// <summary>
    /// Applies the shared response byte budget to resets, dropping the least-urgent entries first
    /// when the estimated payload would overflow <see cref="RealtimeWireLimits.PackingBudgetBytes"/>.
    /// MembershipLost/BeyondRetention are kept preferentially since they indicate the client cannot
    /// recover on its own (unlike GapTooLarge/MessageNotFound/AheadOfTip, which are still actionable
    /// on a later bootstrap).
    /// </summary>
    private static (IReadOnlyList<SyncCursorResetRequired> Kept, int Bytes) PackResetsForBudget(
        IReadOnlyList<SyncCursorResetRequired> resets,
        int responseBytesSoFar)
    {
        if (resets.Count == 0)
            return (resets, 0);

        var ordered = resets
            .OrderBy(static r => ResetPriority(r.Reason))
            .ToArray();

        var kept = new List<SyncCursorResetRequired>(ordered.Length);
        var bytes = 0;
        foreach (var reset in ordered)
        {
            var itemBytes = EstimateResetBytes(reset);
            if (kept.Count > 0 && responseBytesSoFar + bytes + itemBytes > RealtimeWireLimits.PackingBudgetBytes)
                break;

            kept.Add(reset);
            bytes += itemBytes;
        }

        return (kept, bytes);
    }

    private static int ResetPriority(SyncCursorResetReason reason) => reason switch
    {
        SyncCursorResetReason.MembershipLost => 0,
        SyncCursorResetReason.BeyondRetention => 0,
        _ => 1
    };

    private static int EstimateResetBytes(SyncCursorResetRequired reset) =>
        192 // JSON 键名、标点、Reason 枚举、数值字段包装
        + Encoding.UTF8.GetByteCount(reset.ConversationId)
        + Encoding.UTF8.GetByteCount(reset.TipMessageId ?? string.Empty)
        + Encoding.UTF8.GetByteCount(reset.ClientAfterMessageId ?? string.Empty);

    private static int EstimateConversationBytes(ConversationListItem item)
    {
        // Perf-5：旧估算仅计入 ConversationId/LastMessageId/LastMessagePreview，遗漏 Title、
        // PeerUserId、LastSenderUserId、UnreadCount、LastReadMessageId、IsPinned/PinnedAtMs、
        // IsMuted/MutedUntilMs 等。这里补齐所有可能序列化为 JSON 的字段。
        var payload =
            Encoding.UTF8.GetByteCount(item.ConversationId)
            + Encoding.UTF8.GetByteCount(item.Title ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.LastMessageId ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.LastMessagePreview ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.LastReadMessageId ?? string.Empty);
        // JSON 包装开销：键名、引号、冒号、逗号、数值字段（约 12 个数值/布尔字段 × ~16 字节）。
        const int wrapperOverhead = 320;
        return wrapperOverhead + payload + (payload / 8);
    }

    private static int EstimateHistoryBytes(RealtimeHistoryMessage item)
    {
        // Perf-5：旧估算仅计入 MessageId/ClientMessageId/ConversationId/Content，遗漏
        // ReplyToPreview/ForwardedFromPreview/MentionedUserIds/MentionedRoles/Reactions 等。
        var payload =
            Encoding.UTF8.GetByteCount(item.MessageId)
            + Encoding.UTF8.GetByteCount(item.ClientMessageId)
            + Encoding.UTF8.GetByteCount(item.ConversationId ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.Content)
            + Encoding.UTF8.GetByteCount(item.ReplyToMessageId ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.ReplyToPreview ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.ForwardedFromMessageId ?? string.Empty)
            + Encoding.UTF8.GetByteCount(item.ForwardedFromPreview ?? string.Empty);

        var attachmentBytes = 0;
        if (item.Attachments is { Count: > 0 })
        {
            foreach (var attachment in item.Attachments)
            {
                attachmentBytes += 128
                    + Encoding.UTF8.GetByteCount(attachment.AttachmentId)
                    + Encoding.UTF8.GetByteCount(attachment.ContentType)
                    + Encoding.UTF8.GetByteCount(attachment.FileName ?? string.Empty)
                    + Encoding.UTF8.GetByteCount(attachment.DownloadApiHint ?? string.Empty);
            }
        }

        var mentionBytes = 0;
        if (item.MentionedUserIds is { Count: > 0 })
        {
            mentionBytes += item.MentionedUserIds.Count * 16; // long → 最多 20 位数字 + 逗号
        }
        if (item.MentionedRoles is { Count: > 0 })
        {
            foreach (var role in item.MentionedRoles)
                mentionBytes += 16 + Encoding.UTF8.GetByteCount(role);
        }

        var reactionBytes = 0;
        if (item.Reactions is { Count: > 0 })
        {
            foreach (var reaction in item.Reactions)
            {
                // Emoji + Count(int) + ReactedByMe(bool) + JSON 键名/标点
                reactionBytes += 48 + Encoding.UTF8.GetByteCount(reaction.Emoji);
            }
        }

        // JSON 包装开销：键名（约 20 个字段）、引号、冒号、逗号、数值字段。
        const int wrapperOverhead = 384;
        return wrapperOverhead + payload + (payload / 8) + attachmentBytes + mentionBytes + reactionBytes;
    }
}

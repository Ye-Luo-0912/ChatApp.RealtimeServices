using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Projections;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeConversationStore : IRealtimeConversationStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly RealtimeWriteSessionFactory _sessionFactory;

    public NpgsqlRealtimeConversationStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        RealtimeMetrics? metrics = null)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        // Reliability-4：传入 RealtimeMetrics，由 session 在事务提交成功后记录 outbox 入队行数。
        _sessionFactory = new RealtimeWriteSessionFactory(databaseClient, databaseSchema, metrics);
    }

    public async Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default)
    {
        take = Math.Clamp(take, 1, 101);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // 排序键与游标必须一致：置顶 → 置顶时间 → 最后消息时间 → ConversationId。
        // NULLS LAST 用 MinValue 哨兵，保证 keyset 比较与 ORDER BY 语义一致。
        // P0-1：群消息热路径不再更新成员行的 last_message_at_ms，列表排序改用 conversations.last_message_at_ms，
        // 避免群会话因成员行旧值不能及时排到顶部、keyset cursor 不一致。
        const long nullSortSentinel = long.MinValue;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 c.conversation_id,
                 c.type,
                 m.peer_user_id,
                 c.title,
                 c.last_message_id,
                 c.last_message_preview,
                 c.last_message_at_ms,
                 c.last_sender_user_id,
                 -- P0-2：统一未读公式，处理 retention floor 与自发送消息扣除。
                 {ConversationWriteCommands.UnreadCountSqlExpression} AS unread_count,
                 m.last_read_message_id,
                 m.last_read_at_ms,
                 m.is_pinned,
                 m.pinned_at_ms,
                 m.is_muted,
                 m.muted_until_ms
             FROM {_databaseSchema.ConversationMembersTableSql} AS m
             INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                 ON c.conversation_id = m.conversation_id
             WHERE m.user_id = @user_id
               AND m.left_at_ms IS NULL
               AND c.dissolved_at_ms IS NULL
               AND (
                    @before_id IS NULL
                    OR (
                        m.is_pinned::int,
                        COALESCE(m.pinned_at_ms, {nullSortSentinel}),
                        COALESCE(c.last_message_at_ms, {nullSortSentinel}),
                        c.conversation_id
                    ) < (
                        @before_pinned::int,
                        COALESCE(@before_pinned_at, {nullSortSentinel}),
                        COALESCE(@before_at, {nullSortSentinel}),
                        @before_id
                    )
               )
             ORDER BY
                 m.is_pinned DESC,
                 m.pinned_at_ms DESC NULLS LAST,
                 c.last_message_at_ms DESC NULLS LAST,
                 c.conversation_id DESC
             LIMIT @take;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        var beforePinned = command.Parameters.Add("before_pinned", NpgsqlDbType.Boolean);
        beforePinned.Value = (object?)beforeIsPinned ?? DBNull.Value;
        var beforePinnedAt = command.Parameters.Add("before_pinned_at", NpgsqlDbType.Bigint);
        beforePinnedAt.Value = (object?)beforePinnedAtMs ?? DBNull.Value;
        var beforeAt = command.Parameters.Add("before_at", NpgsqlDbType.Bigint);
        beforeAt.Value = (object?)beforeLastMessageAtMs ?? DBNull.Value;
        var beforeId = command.Parameters.Add("before_id", NpgsqlDbType.Text);
        beforeId.Value = (object?)beforeConversationId ?? DBNull.Value;
        command.Parameters.AddWithValue("take", take);

        var items = new List<ConversationListItem>(take);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new ConversationListItem
            {
                ConversationId = reader.GetString(0),
                Type = (ConversationType)reader.GetInt16(1),
                PeerUserId = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                Title = reader.IsDBNull(3) ? null : reader.GetString(3),
                LastMessageId = reader.IsDBNull(4) ? null : reader.GetString(4),
                LastMessagePreview = reader.IsDBNull(5) ? null : reader.GetString(5),
                LastMessageAtMs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                LastSenderUserId = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                UnreadCount = reader.GetInt32(8),
                LastReadMessageId = reader.IsDBNull(9) ? null : reader.GetString(9),
                LastReadAtMs = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                IsPinned = reader.GetBoolean(11),
                PinnedAtMs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                IsMuted = reader.GetBoolean(13),
                MutedUntilMs = reader.IsDBNull(14) ? null : reader.GetInt64(14)
            });
        }

        return items;
    }

    public async Task<IReadOnlyList<ConversationListItem>> QueryArchivedListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default)
    {
        take = Math.Clamp(take, 1, 101);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // 排序键与游标必须一致：置顶 → 置顶时间 → 最后消息时间 → ConversationId。
        // NULLS LAST 用 MinValue 哨兵，保证 keyset 比较与 ORDER BY 语义一致。
        // 二-3：归档列表使用离群快照（left_message_*）优先于 conversations 当前 tip，
        // 避免群在用户离开后继续活跃时泄露离群后的最新预览；离群后未读数固定为 0。
        const long nullSortSentinel = long.MinValue;
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 c.conversation_id,
                 c.type,
                 m.peer_user_id,
                 c.title,
                 COALESCE(m.left_message_id, c.last_message_id) AS last_message_id,
                 COALESCE(m.left_message_preview, c.last_message_preview) AS last_message_preview,
                 COALESCE(m.left_message_at_ms, c.last_message_at_ms) AS last_message_at_ms,
                 COALESCE(m.left_sender_user_id, c.last_sender_user_id) AS last_sender_user_id,
                 -- 离群后未读永远为 0（用户已经离群，不再有未读概念）。
                 0 AS unread_count,
                 m.last_read_message_id,
                 m.last_read_at_ms,
                 m.is_pinned,
                 m.pinned_at_ms,
                 m.is_muted,
                 m.muted_until_ms
             FROM {_databaseSchema.ConversationMembersTableSql} AS m
             INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                 ON c.conversation_id = m.conversation_id
             WHERE m.user_id = @user_id
               AND m.left_at_ms IS NOT NULL
               AND c.type = {(int)ConversationType.Group}
               AND (
                    @before_id IS NULL
                    OR (
                        m.is_pinned::int,
                        COALESCE(m.pinned_at_ms, {nullSortSentinel}),
                        COALESCE(m.left_message_at_ms, c.last_message_at_ms, {nullSortSentinel}),
                        c.conversation_id
                    ) < (
                        @before_pinned::int,
                        COALESCE(@before_pinned_at, {nullSortSentinel}),
                        COALESCE(@before_at, {nullSortSentinel}),
                        @before_id
                    )
               )
             ORDER BY
                 m.is_pinned DESC,
                 m.pinned_at_ms DESC NULLS LAST,
                 COALESCE(m.left_message_at_ms, c.last_message_at_ms) DESC NULLS LAST,
                 c.conversation_id DESC
             LIMIT @take;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        var beforePinned = command.Parameters.Add("before_pinned", NpgsqlDbType.Boolean);
        beforePinned.Value = (object?)beforeIsPinned ?? DBNull.Value;
        var beforePinnedAt = command.Parameters.Add("before_pinned_at", NpgsqlDbType.Bigint);
        beforePinnedAt.Value = (object?)beforePinnedAtMs ?? DBNull.Value;
        var beforeAt = command.Parameters.Add("before_at", NpgsqlDbType.Bigint);
        beforeAt.Value = (object?)beforeLastMessageAtMs ?? DBNull.Value;
        var beforeId = command.Parameters.Add("before_id", NpgsqlDbType.Text);
        beforeId.Value = (object?)beforeConversationId ?? DBNull.Value;
        command.Parameters.AddWithValue("take", take);

        var items = new List<ConversationListItem>(take);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new ConversationListItem
            {
                ConversationId = reader.GetString(0),
                Type = (ConversationType)reader.GetInt16(1),
                PeerUserId = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                Title = reader.IsDBNull(3) ? null : reader.GetString(3),
                LastMessageId = reader.IsDBNull(4) ? null : reader.GetString(4),
                LastMessagePreview = reader.IsDBNull(5) ? null : reader.GetString(5),
                LastMessageAtMs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                LastSenderUserId = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                UnreadCount = reader.GetInt32(8),
                LastReadMessageId = reader.IsDBNull(9) ? null : reader.GetString(9),
                LastReadAtMs = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                IsPinned = reader.GetBoolean(11),
                PinnedAtMs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                IsMuted = reader.GetBoolean(13),
                MutedUntilMs = reader.IsDBNull(14) ? null : reader.GetInt64(14)
            });
        }

        return items;
    }

    public async Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
        long userId,
        string conversationId,
        bool? pinned,
        bool? muted,
        long? mutedUntilMs,
        CancellationToken ct = default)
    {
        // Reliability-4：使用 RealtimeWriteSession 统一事务上下文，
        // Outbox 入队计数在 CommitAsync 成功后才推到 metrics，避免回滚导致 pending 漂移。
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        ConversationType type;
        long? peerUserId;
        string? lastMessageId;
        string? lastMessagePreview;
        long? lastMessageAtMs;
        long? lastSenderUserId;
        bool currentPinned;
        long? currentPinnedAtMs;
        bool currentMuted;
        long? currentMutedUntilMs;

        await using (var load = new NpgsqlCommand(
                           $"""
                            SELECT
                                c.type,
                                m.peer_user_id,
                                c.last_message_id,
                                c.last_message_preview,
                                c.last_message_at_ms,
                                c.last_sender_user_id,
                                m.is_pinned,
                                m.pinned_at_ms,
                                m.is_muted,
                                m.muted_until_ms
                            FROM {session.Schema.ConversationMembersTableSql} AS m
                            INNER JOIN {session.Schema.ConversationsTableSql} AS c
                                ON c.conversation_id = m.conversation_id
                            WHERE m.conversation_id = @conversation_id
                              AND m.user_id = @user_id
                              AND m.left_at_ms IS NULL
                            FOR UPDATE OF m;
                            """,
                           session.Connection,
                           session.Transaction))
        {
            load.Parameters.AddWithValue("conversation_id", conversationId);
            load.Parameters.AddWithValue("user_id", userId);
            await using (var reader = await load.ExecuteReaderAsync(session.CancellationToken).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(session.CancellationToken).ConfigureAwait(false))
                {
                    await reader.CloseAsync().ConfigureAwait(false);
                    await session.RollbackAsync().ConfigureAwait(false);
                    return new ConversationMemberPrefsResult(false, false, false, false, null);
                }

                type = (ConversationType)reader.GetInt16(0);
                peerUserId = reader.IsDBNull(1) ? null : reader.GetInt64(1);
                lastMessageId = reader.IsDBNull(2) ? null : reader.GetString(2);
                lastMessagePreview = reader.IsDBNull(3) ? null : reader.GetString(3);
                lastMessageAtMs = reader.IsDBNull(4) ? null : reader.GetInt64(4);
                lastSenderUserId = reader.IsDBNull(5) ? null : reader.GetInt64(5);
                currentPinned = reader.GetBoolean(6);
                currentPinnedAtMs = reader.IsDBNull(7) ? null : reader.GetInt64(7);
                currentMuted = reader.GetBoolean(8);
                currentMutedUntilMs = reader.IsDBNull(9) ? null : reader.GetInt64(9);
            }
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nextPinned = currentPinned;
        var nextPinnedAtMs = currentPinnedAtMs;
        var nextMuted = currentMuted;
        var nextMutedUntilMs = currentMutedUntilMs;

        if (pinned is true)
        {
            nextPinned = true;
            // 仅 false→true 刷新 pinned_at_ms；重复 pin 不改时间、不产生 Outbox。
            if (!currentPinned)
                nextPinnedAtMs = now;
        }
        else if (pinned is false)
        {
            nextPinned = false;
            nextPinnedAtMs = null;
        }

        if (muted is true)
        {
            nextMuted = true;
            nextMutedUntilMs = mutedUntilMs;
        }
        else if (muted is false)
        {
            nextMuted = false;
            nextMutedUntilMs = null;
        }

        var changed = nextPinned != currentPinned
            || nextPinnedAtMs != currentPinnedAtMs
            || nextMuted != currentMuted
            || nextMutedUntilMs != currentMutedUntilMs;

        if (!changed)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new ConversationMemberPrefsResult(
                true,
                false,
                currentPinned,
                currentMuted,
                currentMutedUntilMs);
        }

        await using (var update = new NpgsqlCommand(
                           $"""
                            UPDATE {session.Schema.ConversationMembersTableSql}
                            SET is_pinned = @is_pinned,
                                pinned_at_ms = @pinned_at_ms,
                                is_muted = @is_muted,
                                muted_until_ms = @muted_until_ms
                            WHERE conversation_id = @conversation_id
                              AND user_id = @user_id;
                            """,
                           session.Connection,
                           session.Transaction))
        {
            update.Parameters.AddWithValue("is_pinned", nextPinned);
            update.Parameters.AddWithValue("pinned_at_ms", (object?)nextPinnedAtMs ?? DBNull.Value);
            update.Parameters.AddWithValue("is_muted", nextMuted);
            update.Parameters.AddWithValue("muted_until_ms", (object?)nextMutedUntilMs ?? DBNull.Value);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", userId);
            await update.ExecuteNonQueryAsync(session.CancellationToken).ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var inserted = await OutboxInsertHelper.InsertAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                ConversationWriteCommands.CreateConversationPrefsChangedEvent(
                    conversationId,
                    userId,
                    type,
                    peerUserId,
                    lastMessageId,
                    lastMessagePreview,
                    lastMessageAtMs,
                    lastSenderUserId,
                    nextPinned,
                    nextPinnedAtMs,
                    nextMuted,
                    nextMutedUntilMs,
                    now,
                    traceParent,
                    traceState),
                session.CancellationToken)
            .ConfigureAwait(false);
        // Reliability-4：累计到 session，由 CommitAsync 在事务提交成功后统一记录到 metrics。
        session.RecordOutboxInsert(inserted);

        await session.CommitAsync().ConfigureAwait(false);
        return new ConversationMemberPrefsResult(
            true,
            true,
            nextPinned,
            nextMuted,
            nextMutedUntilMs);
    }

    public async Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
        long userId,
        string conversationId,
        long? readAtMs,
        string? readMessageId,
        CancellationToken ct = default)
    {
        // Reliability-4：使用 RealtimeWriteSession 统一事务上下文，
        // Outbox 入队计数在 CommitAsync 成功后才推到 metrics，避免回滚导致 pending 漂移。
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        _ = readAtMs; // 权威时间由 messageId 解析；保留参数以兼容调用方。

        long? tipAtMs = null;
        string? tipMessageId = null;
        long tipSequence = 0;
        long? currentReadAtMs = null;
        string? currentReadMessageId = null;
        var currentUnread = 0;
        var memberFound = false;

        await using (var load = new NpgsqlCommand(
                           $"""
                            SELECT
                                c.last_message_at_ms,
                                c.last_message_id,
                                c.last_sequence,
                                m.last_read_at_ms,
                                m.last_read_message_id,
                                -- P0-2：统一未读公式，与列表查询一致（含 retention floor 与自发送消息扣除）。
                                {ConversationWriteCommands.UnreadCountSqlExpression} AS unread_count
                            FROM {session.Schema.ConversationMembersTableSql} AS m
                            INNER JOIN {session.Schema.ConversationsTableSql} AS c
                                ON c.conversation_id = m.conversation_id
                            WHERE m.conversation_id = @conversation_id
                              AND m.user_id = @user_id
                              AND m.left_at_ms IS NULL
                            FOR UPDATE OF m;
                            """,
                           session.Connection,
                           session.Transaction))
        {
            load.Parameters.AddWithValue("conversation_id", conversationId);
            load.Parameters.AddWithValue("user_id", userId);
            await using var reader = await load.ExecuteReaderAsync(session.CancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(session.CancellationToken).ConfigureAwait(false))
            {
                memberFound = true;
                tipAtMs = reader.IsDBNull(0) ? null : reader.GetInt64(0);
                tipMessageId = reader.IsDBNull(1) ? null : reader.GetString(1);
                tipSequence = reader.IsDBNull(2) ? 0 : reader.GetInt64(2);
                currentReadAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
                currentReadMessageId = reader.IsDBNull(4) ? null : reader.GetString(4);
                currentUnread = reader.GetInt32(5);
            }
        }

        if (!memberFound)
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new ConversationReadAdvanceResult(false, false, 0, null, null);
        }

        long targetAtMs;
        string targetMessageId;
        long targetSequence;
        // messageId 为空：推进到 tip。非空：以库内时间为权威，忽略客户端 readAtMs。
        if (string.IsNullOrWhiteSpace(readMessageId))
        {
            if (tipAtMs is null || string.IsNullOrWhiteSpace(tipMessageId))
            {
                await session.CommitAsync().ConfigureAwait(false);
                return new ConversationReadAdvanceResult(
                    true,
                    false,
                    currentUnread,
                    currentReadMessageId,
                    currentReadAtMs);
            }

            targetAtMs = tipAtMs.Value;
            targetMessageId = tipMessageId;
            targetSequence = tipSequence;
        }
        else
        {
            var resolved = await TryResolveReadCursorMessageAsync(
                    session.Connection,
                    session.Transaction,
                    session.Schema,
                    conversationId,
                    readMessageId,
                    session.CancellationToken)
                .ConfigureAwait(false);
            if (resolved is null)
            {
                // 消息不存在或不属于该会话：忽略客户端游标，不前进。
                await session.CommitAsync().ConfigureAwait(false);
                return new ConversationReadAdvanceResult(
                    true,
                    false,
                    currentUnread,
                    currentReadMessageId,
                    currentReadAtMs);
            }

            targetAtMs = resolved.Value.ReceivedAtMs;
            targetMessageId = resolved.Value.MessageId;
            targetSequence = resolved.Value.ConversationSequence ?? tipSequence;

            // 限制在当前会话 tip 以内，防止未来游标把后续消息全部视作已读。
            if (tipAtMs is not null
                && !string.IsNullOrWhiteSpace(tipMessageId)
                && IsMessageAfter(targetAtMs, targetMessageId, tipAtMs.Value, tipMessageId))
            {
                targetAtMs = tipAtMs.Value;
                targetMessageId = tipMessageId;
                targetSequence = tipSequence;
            }
        }

        // Perf-1：使用 O(1) 序列化 MarkRead，消除 COUNT 扫描。
        var readResult = await ConversationWriteCommands.TryAdvanceReadBySequenceAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                conversationId,
                userId,
                targetSequence,
                targetMessageId,
                targetAtMs,
                session.CancellationToken)
            .ConfigureAwait(false);

        if (!readResult.Advanced)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new ConversationReadAdvanceResult(
                true,
                false,
                readResult.UnreadCount,
                currentReadMessageId,
                currentReadAtMs);
        }

        var unread = readResult.UnreadCount;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        // 读者自身的未读变更事件：每个用户绝对未读数不同，必须逐用户。
        var readerUnreadEvent = ConversationWriteCommands.CreateUnreadCountChangedEvent(
            conversationId,
            userId,
            unread,
            targetMessageId,
            targetAtMs,
            causeMessageId: targetMessageId,
            now,
            traceParent,
            traceState);

        var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                conversationId,
                session.CancellationToken)
            .ConfigureAwait(false);

        // Perf-9：群已读走统一 GroupProjectionDelta 协议，ConversationRead 广播聚合为单行 Outbox。
        if (ConversationId.IsGroup(conversationId))
        {
            var delta = new GroupProjectionDelta(conversationId, memberIds);

            // 排除读者本人：读者不需要再收到自己的已读水位通知。
            var others = new List<long>(memberIds.Count);
            foreach (var memberId in memberIds)
            {
                if (memberId != userId)
                    others.Add(memberId);
            }

            // 通知群内其他成员某用户已读到某水位：单一 payload 适合广播。
            delta.AddBroadcastTo(
                GroupProjectionEventFactory.CreateGroupConversationReadBroadcast(
                    conversationId,
                    userId,
                    targetMessageId,
                    targetAtMs,
                    now,
                    targetSequence,
                    traceParent,
                    traceState),
                others);

            // 读者自身的未读数变更保持逐用户。
            delta.AddPerUser(readerUnreadEvent);

            var groupInserted = await OutboxInsertHelper.InsertManyAsync(
                    session.Connection,
                    session.Transaction,
                    session.Schema,
                    delta.Build(),
                    session.CancellationToken)
                .ConfigureAwait(false);
            // Reliability-4：累计到 session，由 CommitAsync 在事务提交成功后统一记录到 metrics。
            session.RecordOutboxInsert(groupInserted);
        }
        else
        {
            // 单聊路径：保持 per-target ConversationRead 事件 + 读者未读变更。
            var events = new List<RealtimeEvent>(memberIds.Count + 1) { readerUnreadEvent };
            foreach (var memberId in memberIds)
            {
                if (memberId == userId)
                    continue;

                events.Add(ConversationWriteCommands.CreateConversationReadEvent(
                    conversationId,
                    memberId,
                    userId,
                    targetMessageId,
                    targetAtMs,
                    now,
                    targetSequence,
                    traceParent,
                    traceState));
            }

            var directInserted = await OutboxInsertHelper.InsertManyAsync(
                    session.Connection,
                    session.Transaction,
                    session.Schema,
                    events,
                    session.CancellationToken)
                .ConfigureAwait(false);
            session.RecordOutboxInsert(directInserted);
        }

        await session.CommitAsync().ConfigureAwait(false);
        return new ConversationReadAdvanceResult(
            true,
            true,
            unread,
            targetMessageId,
            targetAtMs);
    }

    private async Task<(long ReceivedAtMs, string MessageId, long? ConversationSequence)?> TryResolveReadCursorMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string conversationId,
        string messageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT received_at_ms, message_id, conversation_sequence
             FROM {schema.MessagesTableSql}
             WHERE conversation_id = @conversation_id
               AND message_id = @message_id
             LIMIT 1;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("message_id", messageId.Trim());
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return (reader.GetInt64(0), reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2));
    }

    private static bool IsMessageAfter(
        long candidateAtMs,
        string candidateMessageId,
        long tipAtMs,
        string tipMessageId) =>
        candidateAtMs > tipAtMs
        || (candidateAtMs == tipAtMs
            && string.CompareOrdinal(candidateMessageId, tipMessageId) > 0);
}

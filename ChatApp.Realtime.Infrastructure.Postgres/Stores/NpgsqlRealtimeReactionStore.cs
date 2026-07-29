using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Projections;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeReactionStore : IRealtimeReactionStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly IConversationMessageMutationPolicy _mutationPolicy;
    private readonly RealtimeWriteSessionFactory _sessionFactory;

    public NpgsqlRealtimeReactionStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        IConversationMessageMutationPolicy mutationPolicy,
        RealtimeMetrics? metrics = null)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _mutationPolicy = mutationPolicy;
        // Reliability-4：传入 RealtimeMetrics，由 session 在事务提交成功后记录 outbox 入队行数。
        _sessionFactory = new RealtimeWriteSessionFactory(databaseClient, databaseSchema, metrics);
    }

    public async Task<MessageReactionPersistResult> AddAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        MessageReactionOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(emoji);
        ArgumentException.ThrowIfNullOrWhiteSpace(actorSessionId);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(actorUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(occurredAtMs);

        // Reliability-4：使用 RealtimeWriteSession 统一事务上下文，
        // Outbox 入队计数在 CommitAsync 成功后才推到 metrics，避免回滚导致 pending 漂移。
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期，防止已注销用户添加 reaction。
        if (!await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                session.Connection, session.Transaction, session.Schema, actorUserId, session.CancellationToken)
            .ConfigureAwait(false))
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed, messageId);
        }

        var access = await TryLockMessageAccessAsync(
                session.Connection,
                session.Transaction,
                messageId,
                session.CancellationToken)
            .ConfigureAwait(false);
        if (access is null)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotFound,
                messageId);
        }

        if (access.RecalledAtMs is not null)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.AlreadyRecalled,
                messageId,
                access.ConversationId,
                emoji);
        }

        // P0-8：群消息 Reaction 需验证操作者仍是当前群成员，防离群后修改旧消息。
        var addAuth = await _mutationPolicy
            .AuthorizeMutationAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                new MessageMutationContext(
                    access.ConversationId,
                    access.SenderUserId,
                    access.ReceiverUserId,
                    actorUserId,
                    MessageMutationOperation.Reaction),
                session.CancellationToken)
            .ConfigureAwait(false);
        if (!addAuth.Allowed)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed,
                messageId,
                access.ConversationId,
                emoji);
        }

        // Perf-10：将 exists/count/limit/insert/bump/final-count 压成单条 CTE，7 次往返→1 次。
        // PostgreSQL 数据修改 CTE 不互相可见目标表变更，但可读 RETURNING 输出；
        // 最终 emoji_count 由 pre-insert 计数 + 是否插入调整得出。
        var (addStatus, emojiCount) = await TryAddReactionCteAsync(
                session.Connection,
                session.Transaction,
                messageId,
                actorUserId,
                emoji,
                occurredAtMs,
                options.MaxReactionsPerUserPerMessage,
                options.MaxDistinctEmojisPerMessage,
                session.CancellationToken)
            .ConfigureAwait(false);

        if (addStatus == ReactionAddStatus.AlreadyExists)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                messageId,
                access.ConversationId,
                emoji,
                occurredAtMs,
                emojiCount);
        }

        if (addStatus == ReactionAddStatus.LimitExceeded)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.LimitExceeded,
                messageId,
                access.ConversationId,
                emoji);
        }

        await InsertReactionEventsAsync(
                session,
                added: true,
                messageId,
                access.ConversationId,
                actorUserId,
                actorSessionId,
                access.SenderUserId,
                access.ReceiverUserId,
                emoji,
                emojiCount,
                occurredAtMs,
                access.ConversationSequence)
            .ConfigureAwait(false);

        await session.CommitAsync().ConfigureAwait(false);
        return new MessageReactionPersistResult(
            MessageReactionPersistStatus.Applied,
            messageId,
            access.ConversationId,
            emoji,
            occurredAtMs,
            emojiCount);
    }

    public async Task<MessageReactionPersistResult> RemoveAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(emoji);
        ArgumentException.ThrowIfNullOrWhiteSpace(actorSessionId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(actorUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(occurredAtMs);

        // Reliability-4：使用 RealtimeWriteSession 统一事务上下文，
        // Outbox 入队计数在 CommitAsync 成功后才推到 metrics，避免回滚导致 pending 漂移。
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期，防止已注销用户移除 reaction。
        if (!await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                session.Connection, session.Transaction, session.Schema, actorUserId, session.CancellationToken)
            .ConfigureAwait(false))
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed, messageId);
        }

        var access = await TryLockMessageAccessAsync(
                session.Connection,
                session.Transaction,
                messageId,
                session.CancellationToken)
            .ConfigureAwait(false);
        if (access is null)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotFound,
                messageId);
        }

        if (access.RecalledAtMs is not null)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.AlreadyRecalled,
                messageId,
                access.ConversationId,
                emoji);
        }

        // P0-8：群消息 Reaction 移除同样需验证操作者仍是当前群成员。
        var removeAuth = await _mutationPolicy
            .AuthorizeMutationAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                new MessageMutationContext(
                    access.ConversationId,
                    access.SenderUserId,
                    access.ReceiverUserId,
                    actorUserId,
                    MessageMutationOperation.Reaction),
                session.CancellationToken)
            .ConfigureAwait(false);
        if (!removeAuth.Allowed)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed,
                messageId,
                access.ConversationId,
                emoji);
        }

        // Perf-10：DELETE + bump + count 压成单条 CTE，3 次往返→1 次。
        var (removed, emojiCount) = await TryRemoveReactionCteAsync(
                session.Connection,
                session.Transaction,
                messageId,
                actorUserId,
                emoji,
                occurredAtMs,
                session.CancellationToken)
            .ConfigureAwait(false);

        if (!removed)
        {
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                messageId,
                access.ConversationId,
                emoji,
                occurredAtMs,
                emojiCount);
        }

        await InsertReactionEventsAsync(
                session,
                added: false,
                messageId,
                access.ConversationId,
                actorUserId,
                actorSessionId,
                access.SenderUserId,
                access.ReceiverUserId,
                emoji,
                emojiCount,
                occurredAtMs,
                access.ConversationSequence)
            .ConfigureAwait(false);

        await session.CommitAsync().ConfigureAwait(false);
        return new MessageReactionPersistResult(
            MessageReactionPersistStatus.Applied,
            messageId,
            access.ConversationId,
            emoji,
            occurredAtMs,
            emojiCount);
    }

    public async Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(messageIds);
        if (messageIds.Count == 0)
            return [];

        var ids = messageIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (ids.Length == 0)
            return [];

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT message_id, user_id, emoji, created_at_ms
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = ANY(@message_ids)
             ORDER BY message_id, created_at_ms, user_id, emoji;
             """,
            connection);
        var param = command.Parameters.Add("message_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        param.Value = ids;

        var rows = new List<MessageReactionRecord>(ids.Length);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new MessageReactionRecord
            {
                MessageId = reader.GetString(0),
                UserId = reader.GetInt64(1),
                Emoji = reader.GetString(2),
                CreatedAtMs = reader.GetInt64(3)
            });
        }

        return rows;
    }

    /// <summary>
    /// 六-4：账号清理时删除该用户的全部反应记录。
    /// </summary>
    public async Task<int> DeleteByUserAsync(long userId, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE user_id = @user_id;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<MessageAccess?> TryLockMessageAccessAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        CancellationToken ct)
    {
        long senderUserId;
        long receiverUserId;
        string? conversationId;
        long? recalledAtMs;
        long? conversationSequence;

        // 七-3：不再 FOR UPDATE 锁 messages 正文行（避免阻塞编辑/撤回等其他写入）。
        // 1. 无锁读取 messages 行获取路由信息（不锁正文行）
        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, conversation_id, recalled_at_ms,
                                 conversation_sequence
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return null;

            senderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            conversationId = reader.IsDBNull(2) ? null : reader.GetString(2);
            recalledAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
            conversationSequence = reader.IsDBNull(4) ? null : reader.GetInt64(4);
        }

        // 2. 确保状态行存在（Migration042 引入的 message_state 表）
        await using (var ensureCmd = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_databaseSchema.MessageStateTableSql} ("message_id", "changed_at_ms")
                          VALUES (@message_id, 0)
                          ON CONFLICT ("message_id") DO NOTHING
                          """,
                         connection,
                         transaction))
        {
            ensureCmd.Parameters.AddWithValue("message_id", messageId);
            await ensureCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 3. 锁独立状态行（不锁 messages 正文行），串行化同一消息上的 Reaction 操作
        await using (var lockCmd = new NpgsqlCommand(
                         $"""
                          SELECT 1 FROM {_databaseSchema.MessageStateTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         connection,
                         transaction))
        {
            lockCmd.Parameters.AddWithValue("message_id", messageId);
            await using var lockReader = await lockCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await lockReader.ReadAsync(ct).ConfigureAwait(false))
                return null;
        }

        return new MessageAccess(
            senderUserId,
            receiverUserId,
            conversationId,
            recalledAtMs,
            conversationSequence);
    }

    /// <summary>
    /// Perf-10：Add Reaction 单 CTE。将 exists 检查、用户上限、emoji 去重上限、INSERT、bump changed_at、
    /// 最终 emoji 计数压成一次数据库往返。利用 (message_id, emoji) 索引加速 emoji 过滤。
    /// </summary>
    /// <remarks>
    /// PostgreSQL 数据修改 CTE 不互相可见目标表变更，但可读 RETURNING 输出。
    /// 最终 emoji_count = pre_count + (是否插入 ? 1 : 0)。
    /// pre_count 在 INSERT 之前读取，包含当前用户已有的同 emoji 反应（若已存在则 INSERT 会被 ON CONFLICT 跳过）。
    /// </remarks>
    private async Task<(ReactionAddStatus status, int emojiCount)> TryAddReactionCteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long userId,
        string emoji,
        long occurredAtMs,
        int maxPerUser,
        int maxDistinctEmojis,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             WITH
             existing AS (
                 SELECT 1 FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id = @user_id AND emoji = @emoji
                 LIMIT 1
             ),
             user_cnt AS (
                 SELECT COUNT(*)::int AS v FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id = @user_id
             ),
             emoji_exists_other AS (
                 SELECT 1 FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND emoji = @emoji AND user_id <> @user_id
                 LIMIT 1
             ),
             distinct_other AS (
                 SELECT COUNT(DISTINCT emoji)::int AS v FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id <> @user_id
             ),
             emoji_count_pre AS (
                 SELECT COUNT(*)::int AS v FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND emoji = @emoji
             ),
             decision AS (
                 SELECT CASE
                     WHEN EXISTS(SELECT 1 FROM existing) THEN 0
                     WHEN (SELECT v FROM user_cnt) >= @max_per_user THEN 1
                     WHEN NOT EXISTS(SELECT 1 FROM emoji_exists_other)
                          AND (SELECT v FROM distinct_other) >= @max_distinct THEN 1
                     ELSE 2
                 END AS status
             ),
             ins AS (
                 INSERT INTO {_databaseSchema.MessageReactionsTableSql}
                     (message_id, user_id, emoji, created_at_ms)
                 SELECT @message_id, @user_id, @emoji, @created_at_ms
                 WHERE (SELECT status FROM decision) = 2
                 ON CONFLICT (message_id, user_id, emoji) DO NOTHING
                 RETURNING 1
             ),
             bump AS (
                 UPDATE {_databaseSchema.MessagesTableSql}
                 SET changed_at_ms = GREATEST(changed_at_ms, @changed_at_ms)
                 WHERE message_id = @message_id AND EXISTS (SELECT 1 FROM ins)
             )
             SELECT
                 CASE
                     WHEN EXISTS(SELECT 1 FROM ins) THEN 2
                     ELSE (SELECT status FROM decision)
                 END,
                 (SELECT v FROM emoji_count_pre) +
                     CASE WHEN EXISTS(SELECT 1 FROM ins) THEN 1 ELSE 0 END;
             """,
            connection,
            transaction);

        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("emoji", emoji);
        command.Parameters.AddWithValue("created_at_ms", occurredAtMs);
        command.Parameters.AddWithValue("changed_at_ms", occurredAtMs);
        command.Parameters.AddWithValue("max_per_user", maxPerUser);
        command.Parameters.AddWithValue("max_distinct", maxDistinctEmojis);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return (ReactionAddStatus.LimitExceeded, 0);

        var status = reader.GetInt32(0);
        var count = reader.GetInt32(1);
        return (status switch
        {
            0 => ReactionAddStatus.AlreadyExists,
            1 => ReactionAddStatus.LimitExceeded,
            _ => ReactionAddStatus.Inserted
        }, count);
    }

    /// <summary>
    /// Perf-10：Remove Reaction 单 CTE。DELETE + bump changed_at + emoji 计数压成一次往返。
    /// </summary>
    private async Task<(bool deleted, int emojiCount)> TryRemoveReactionCteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long userId,
        string emoji,
        long occurredAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             WITH
             emoji_count_pre AS (
                 SELECT COUNT(*)::int AS v FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND emoji = @emoji
             ),
             del AS (
                 DELETE FROM {_databaseSchema.MessageReactionsTableSql}
                 WHERE message_id = @message_id AND user_id = @user_id AND emoji = @emoji
                 RETURNING 1
             ),
             bump AS (
                 UPDATE {_databaseSchema.MessagesTableSql}
                 SET changed_at_ms = GREATEST(changed_at_ms, @changed_at_ms)
                 WHERE message_id = @message_id AND EXISTS (SELECT 1 FROM del)
             )
             SELECT
                 EXISTS(SELECT 1 FROM del),
                 (SELECT v FROM emoji_count_pre) -
                     CASE WHEN EXISTS(SELECT 1 FROM del) THEN 1 ELSE 0 END;
             """,
            connection,
            transaction);

        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("emoji", emoji);
        command.Parameters.AddWithValue("changed_at_ms", occurredAtMs);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return (false, 0);

        var deleted = reader.GetBoolean(0);
        var count = reader.GetInt32(1);
        return (deleted, count);
    }

    private async Task InsertReactionEventsAsync(
        RealtimeWriteSession session,
        bool added,
        string messageId,
        string? conversationId,
        long reactorUserId,
        string reactorSessionId,
        long messageSenderUserId,
        long messageReceiverUserId,
        string emoji,
        int emojiCount,
        long occurredAtMs,
        long? conversationSequence)
    {
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        // Perf-9：群反应走统一 GroupProjectionDelta 协议，广播事件聚合为单行 Outbox。
        if (!string.IsNullOrWhiteSpace(conversationId)
            && ConversationId.IsGroup(conversationId))
        {
            var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                    session.Connection,
                    session.Transaction,
                    session.Schema,
                    conversationId,
                    session.CancellationToken)
                .ConfigureAwait(false);

            var delta = new GroupProjectionDelta(conversationId, memberIds);
            delta.AddBroadcast(GroupProjectionEventFactory.CreateGroupReactionBroadcast(
                added,
                messageId,
                conversationId,
                reactorUserId,
                reactorSessionId,
                messageSenderUserId,
                messageReceiverUserId,
                emoji,
                emojiCount,
                occurredAtMs,
                conversationSequence,
                traceParent,
                traceState));

            var inserted = await OutboxInsertHelper.InsertManyAsync(
                    session.Connection,
                    session.Transaction,
                    session.Schema,
                    delta.Build(),
                    session.CancellationToken)
                .ConfigureAwait(false);
            // Reliability-4：累计到 session，由 CommitAsync 在事务提交成功后统一记录到 metrics。
            session.RecordOutboxInsert(inserted);
            return;
        }

        // 单聊路径：保持 per-target 事件（发送方 + 接收方），各产生独立 Outbox 行。
        var directTargets = new HashSet<long> { messageSenderUserId, messageReceiverUserId };
        string payloadJson;
        RealtimeEventType eventType;
        if (added)
        {
            eventType = RealtimeEventType.ReactionAdded;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionAddedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs,
                    ConversationSequence = conversationSequence
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionAddedPayload);
        }
        else
        {
            eventType = RealtimeEventType.ReactionRemoved;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionRemovedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionRemovedPayload);
        }

        var events = new List<RealtimeEvent>(directTargets.Count);
        foreach (var targetUserId in directTargets)
        {
            var eventId = added
                ? MessageEventIdFactory.CreateReactionAddedEventId(
                    messageId,
                    targetUserId,
                    reactorUserId,
                    emoji,
                    occurredAtMs)
                : MessageEventIdFactory.CreateReactionRemovedEventId(
                    messageId,
                    targetUserId,
                    reactorUserId,
                    emoji,
                    occurredAtMs);

            events.Add(new RealtimeEvent
            {
                EventId = eventId,
                Type = eventType,
                TargetUserId = targetUserId,
                ActorUserId = reactorUserId,
                MessageId = messageId,
                SessionId = reactorSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = occurredAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
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

    private sealed record MessageAccess(
        long SenderUserId,
        long ReceiverUserId,
        string? ConversationId,
        long? RecalledAtMs,
        long? ConversationSequence);

    private enum ReactionAddStatus
    {
        AlreadyExists,
        LimitExceeded,
        Inserted
    }
}

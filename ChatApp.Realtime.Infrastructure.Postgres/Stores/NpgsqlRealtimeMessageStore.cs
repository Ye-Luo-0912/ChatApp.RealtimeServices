using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Attachments;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Conversations;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messages;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using ChatApp.Realtime.Infrastructure.Postgres.Projections;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// <see cref="IRealtimeMessageStore"/> 的 Npgsql 直连实现。
/// <para>
/// 本类只负责业务编排：在每个公共方法入口打开一个 <see cref="RealtimeWriteSession"/>，
/// 然后把消息写入、附件绑定、会话投影、未读数、Outbox 发布等职责委派给独立的 Writer，
/// 最后提交事务。SQL 与事件构造分别下沉到 <see cref="MessageWriter"/> /
/// <see cref="MessageMutationWriter"/> / <see cref="ConversationProjectionWriter"/> /
/// <see cref="AttachmentBindingWriter"/> / <see cref="PostgresOutboxWriter"/> /
/// <see cref="RealtimeMessageEventFactory"/>，单事务一致性不变、不增加数据库往返。
/// </para>
/// <para>
/// Perf-3：Incoming 消息热路径合并为单事务。<see cref="ICommandIdempotencyLedger"/> 的
/// 查询与记录下沉到 <see cref="SaveAsync"/> 事务内，消除 Processor 中 2 次独立连接获取。
/// ledger 参数为可选（默认 null）：未注入时回退到仅依赖 messages 表唯一索引的幂等行为
/// （测试与未配置 PostgreSQL 场景）；生产 DI 路径由 <c>RealtimePostgresRegistration</c>
/// 注入 <see cref="NpgsqlCommandIdempotencyLedger"/>。
/// </para>
/// </summary>
public sealed class NpgsqlRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly RealtimeWriteSessionFactory _sessionFactory;
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly IConversationMessageMutationPolicy _mutationPolicy;
    private readonly ICommandIdempotencyLedger? _idempotencyLedger;
    private readonly ILogger<NpgsqlRealtimeMessageStore> _logger;

    public NpgsqlRealtimeMessageStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        IConversationMessageMutationPolicy mutationPolicy,
        ILogger<NpgsqlRealtimeMessageStore> logger,
        RealtimeMetrics? metrics = null,
        ICommandIdempotencyLedger? idempotencyLedger = null)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _mutationPolicy = mutationPolicy;
        // Reliability-4：传入 RealtimeMetrics，由 session 在事务提交成功后记录 outbox 入队行数。
        _sessionFactory = new RealtimeWriteSessionFactory(databaseClient, databaseSchema, metrics);
        _logger = logger;
        // Perf-3：可选注入幂等账本，未注入时 SaveAsync 跳过 ledger 操作。
        _idempotencyLedger = idempotencyLedger;
    }

    public async Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        // P0-10：优先使用 Processor 在 sanitization 之前基于原始请求计算的指纹；
        // 未提供时回退到基于 sanitized mentions 重算（向后兼容）。
        var fingerprint = message.RequestFingerprint ?? RealtimeMessageFingerprint.Compute(
            message.ReceiverUserId,
            message.Content,
            message.AttachmentIds,
            message.ConversationId,
            message.ReplyToMessageId,
            message.ForwardedFromMessageId,
            message.MentionedUserIds,
            message.MentionedRoles,
            message.ReplyToSenderUserId,
            message.ReplyToPreview,
            message.ForwardedFromSenderUserId,
            message.ForwardedFromPreview);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：批量在事务内获取发送方 + 接收方 advisory lock（共享）并检查生命周期状态。
        // 按 userId 升序获取锁，消除 A→B 与 B→A 并发写入之间的死锁环。
        // 消除"processor 预检查 tombstone → 开消息事务 → 提交"之间的 TOCTOU 竞态。
        // 单聊同时锁接收方，防止接收方正在被删除时写入孤儿消息。
        var lifecycleUserIds = message.ReceiverUserId > 0
            ? new[] { message.SenderUserId, message.ReceiverUserId }
            : new[] { message.SenderUserId };
        var lifecycleGate = await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveManyAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                lifecycleUserIds,
                ct).ConfigureAwait(false);
        if (!lifecycleGate.IsActive)
        {
            await session.RollbackAsync().ConfigureAwait(false);
            // 三-3：区分 Frozen 与 Deleted 返回不同错误码，使 Processor 能映射 user_frozen。
            var isFrozen = lifecycleGate.ErrorCode == "user_frozen";
            _logger.LogWarning(
                "入站消息被拒绝：发送或接收用户{Reason}。发送用户={SenderUserId}；接收用户={ReceiverUserId}；消息={MessageId}",
                isFrozen ? "已冻结" : "已注销",
                message.SenderUserId,
                message.ReceiverUserId,
                message.MessageId);
            return isFrozen
                ? RealtimeMessagePersistResult.UserFrozen(message.MessageId)
                : RealtimeMessagePersistResult.UserDeleted(message.MessageId);
        }

        // Perf-3：在事务内查询独立幂等账本，消除 Processor 中事务外的 FindAsync 连接获取。
        // 账本 canonical 解耦幂等性依据与 messages 行生命周期：消息行被 retention GC 或
        // 账号删除清理后，账本仍保留命令处理结果，防止 JetStream replay 将旧命令当作新消息重新写入。
        if (_idempotencyLedger is not null)
        {
            var ledgerEntry = await _idempotencyLedger
                .FindInTransactionAsync(
                    session.Connection,
                    session.Transaction,
                    message.SenderUserId,
                    message.ClientMessageId,
                    ct)
                .ConfigureAwait(false);
            if (ledgerEntry is not null)
            {
                if (string.Equals(ledgerEntry.ContentFingerprint, fingerprint, StringComparison.Ordinal))
                {
                    // 幂等重放：内容指纹匹配。ledger 是 canonical，消息可能已被 retention GC 清理。
                    // 不写消息，直接提交空事务（仅释放 advisory lock）。
                    await session.CommitAsync().ConfigureAwait(false);
                    _logger.LogDebug(
                        "入站消息账本命中（重放）。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                        message.ClientMessageId,
                        message.SenderUserId,
                        ledgerEntry.MessageId);
                    return RealtimeMessagePersistResult.Duplicate(
                        ledgerEntry.MessageId ?? message.MessageId);
                }

                // 内容冲突：指纹不一致。回滚事务（advisory lock 释放），不覆盖 canonical。
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    "入站消息账本命中（冲突）。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                    message.ClientMessageId,
                    message.SenderUserId,
                    ledgerEntry.MessageId);
                return RealtimeMessagePersistResult.Conflict(
                    ledgerEntry.MessageId ?? message.MessageId);
            }
        }

        var messageWriter = new MessageWriter(session);
        var attachmentWriter = new AttachmentBindingWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        // 极限-2：序列分配前置。在 INSERT messages 之前调用 TryAllocate...Async 取得
        // (conversation_sequence, sender_sequence)，传入 INSERT 直接填入对应列，
        // 消除旧流程"INSERT NULL → UPDATE 回写"产生的第二个 tuple 与额外 WAL/索引写入。
        // 群路径授权失败（会话不存在/非群/已解散/发送者非活跃成员）时 allocation 返回 null，
        // 此时直接返回 NotAllowed，不再走"INSERT → 回滚"的弯路。
        var isGroup = !string.IsNullOrWhiteSpace(message.ConversationId)
                      && ConversationId.IsGroup(message.ConversationId);

        long? conversationSequence = null;
        long? senderSequence = null;

        if (isGroup)
        {
            var allocation = await conversationWriter
                .TryAllocateGroupSequenceAsync(
                    message.ConversationId!,
                    message.SenderUserId,
                    message.MessageId,
                    ConversationId.CreatePreview(message.Content),
                    message.ReceivedAtMs)
                .ConfigureAwait(false);

            if (allocation is null)
            {
                // 授权失败：会话不存在、非群、已解散或发送者非活跃成员。无 INSERT 需回滚。
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    "群消息写入拒绝：会话不存在、非群、已解散或发送者非活跃成员。消息={MessageId}；会话={ConversationId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.ConversationId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.NotAllowed(message.MessageId);
            }

            conversationSequence = allocation.ConversationSequence;
            senderSequence = allocation.SenderSequence;
        }
        else if (!string.IsNullOrWhiteSpace(message.ConversationId))
        {
            // 单聊路径：allocation 通常不会失败（UPSERT 语义），失败回退到 NULL 序列。
            var allocation = await conversationWriter
                .TryAllocateDirectSequenceAsync(
                    message.ConversationId!,
                    message.SenderUserId,
                    message.ReceiverUserId,
                    message.MessageId,
                    ConversationId.CreatePreview(message.Content),
                    message.ReceivedAtMs)
                .ConfigureAwait(false);
            if (allocation is not null)
            {
                conversationSequence = allocation.ConversationSequence;
                senderSequence = allocation.SenderSequence;
            }
        }

        var affectedRows = await messageWriter
            .InsertAsync(message, fingerprint, conversationSequence, senderSequence)
            .ConfigureAwait(false);
        if (affectedRows == 0)
        {
            var existing = await messageWriter
                .GetExistingForIdempotencyAsync(message.SenderUserId, message.ClientMessageId)
                .ConfigureAwait(false);
            var existingAttachmentIds = await messageWriter
                .ListAttachmentIdsAsync(existing.MessageId)
                .ConfigureAwait(false);
            if (!RealtimeMessageFingerprint.MatchesExisting(
                    existing.Fingerprint,
                    existing.ReceiverUserId,
                    existing.Content,
                    existingAttachmentIds,
                    fingerprint,
                    existing.ConversationId,
                    existing.ReplyToMessageId,
                    existing.ForwardedFromMessageId,
                    existing.MentionedUserIds,
                    existing.MentionedRoles,
                    existing.ReplyToSenderUserId,
                    existing.ReplyToPreview,
                    existing.ForwardedFromSenderUserId,
                    existing.ForwardedFromPreview))
            {
                // Perf-3：messages 表冲突但账本未命中（迁移前数据），事务内回填账本 canonical。
                // COMMIT 而非 ROLLBACK：消息 INSERT 是 0 行（无变更），仅持久化 ledger 行。
                // 事务内原子性保证：ledger 写入失败则整个事务回滚（更正确的行为）。
                if (_idempotencyLedger is not null)
                {
                    await _idempotencyLedger
                        .RecordInTransactionAsync(
                            session.Connection,
                            session.Transaction,
                            message.MessageId,
                            message.SenderUserId,
                            message.ClientMessageId,
                            fingerprint,
                            IdempotencyLedgerResultKind.Conflict,
                            existing.MessageId,
                            message.ReceivedAtMs,
                            ct)
                        .ConfigureAwait(false);
                }
                await session.CommitAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                    message.ClientMessageId,
                    message.SenderUserId,
                    existing.MessageId);
                return RealtimeMessagePersistResult.Conflict(existing.MessageId);
            }

            // Perf-3：messages 表命中但账本未命中（迁移前数据），事务内回填账本 canonical。
            if (_idempotencyLedger is not null)
            {
                await _idempotencyLedger
                    .RecordInTransactionAsync(
                        session.Connection,
                        session.Transaction,
                        message.MessageId,
                        message.SenderUserId,
                        message.ClientMessageId,
                        fingerprint,
                        IdempotencyLedgerResultKind.Duplicate,
                        existing.MessageId,
                        message.ReceivedAtMs,
                        ct)
                    .ConfigureAwait(false);
            }

            // 消息与 Outbox 同事务提交：重复投递不重建 Published/Dead（及清理后的）Outbox 行。
            await session.CommitAsync().ConfigureAwait(false);
            _logger.LogDebug(
                "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return RealtimeMessagePersistResult.Duplicate(existing.MessageId);
        }

        IReadOnlyList<AttachmentRef>? boundAttachmentRefs = null;
        if (message.AttachmentIds is { Count: > 0 })
        {
            var expected = message.AttachmentIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.Ordinal)
                .Count();
            try
            {
                var boundRecords = await attachmentWriter
                    .BindConfirmedToMessageAsync(
                        message.MessageId,
                        message.ConversationId,
                        message.SenderUserId,
                        message.AttachmentIds)
                    .ConfigureAwait(false);
                if (boundRecords.Count != expected)
                {
                    await session.RollbackAsync().ConfigureAwait(false);
                    _logger.LogWarning(
                        "附件绑定失败。消息={MessageId}；期望={Expected}；实际={Bound}",
                        message.MessageId,
                        expected,
                        boundRecords.Count);
                    return RealtimeMessagePersistResult.AttachmentBindFailed(message.MessageId);
                }

                boundAttachmentRefs = AttachmentRefMapper.FromRecords(boundRecords);
            }
            catch (InvalidOperationException ex)
            {
                await session.RollbackAsync().ConfigureAwait(false);
                _logger.LogWarning(
                    ex,
                    "附件绑定拒绝。消息={MessageId}；发送用户={SenderUserId}",
                    message.MessageId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.AttachmentBindFailed(message.MessageId);
            }
        }

        // 极限-2：序列已在 INSERT 前分配并写入 messages 行。此处仅构造 Outbox 事件。
        if (isGroup)
        {
            // 序列号已知后物化 payload，使 PayloadJson 携带 ConversationSequence。
            var createdEvent = RealtimeMessageEventFactory.EnrichChatMessagePayload(
                RealtimeMessageEventFactory.CopyWithMessageId(eventToPublish, message.MessageId),
                boundAttachmentRefs,
                conversationSequence,
                ConversationType.Group);

            // P0-3：群广播事件 AudienceKind=Conversation + ConversationId，TargetUserIds=null。
            // Publisher 通过 IConversationGatewayDirectory 一次查询会话在线 Gateway 实例集合投递，
            // 不再物化成员数组。GroupProjectionDelta 构造时不传 memberUserIds 即为广播模式。
            var delta = new GroupProjectionDelta(message.ConversationId!);
            delta.AddBroadcast(RealtimeMessageEventFactory.CreateGroupMessageAggregatedEvent(
                createdEvent,
                message,
                targetUserIds: null));

            // 极限-1：不再生成单独的 ConversationListChanged 广播行——payload 已携带
            // ConversationType + ConversationSequence，客户端从单条 MessageReceived 即可更新会话列表。
            await outboxWriter.InsertManyAsync(delta.Build()).ConfigureAwait(false);
        }
        else
        {
            // P0-1：写入路径不再物化接收方 unread_count，也不再发送 per-user UnreadCountChanged 事件。
            // 客户端通过 MessageReceived / ConversationChanged 中的 last_sequence 自行推导未读数变化。

            // 序列号已知后物化 payload，使 PayloadJson 携带 ConversationSequence。
            var createdEvent = RealtimeMessageEventFactory.EnrichChatMessagePayload(
                RealtimeMessageEventFactory.CopyWithMessageId(eventToPublish, message.MessageId),
                boundAttachmentRefs,
                conversationSequence,
                ConversationType.Direct);

            // 极限-1：单聊 4 行 → 2 行。payload 已携带 ConversationType + ConversationSequence，
            // 客户端从 MessageReceived 即可更新会话列表，不再生成双方 ConversationChanged 行。
            // 保留 receiver MessageReceived + sender echo：单聊投递必须按 TargetUserId 精确路由（隐私边界）。
            var outboxEvents = new List<RealtimeEvent>(2) { createdEvent };

            // 发送方其他在线设备回声：同事务写入；Gateway 会跳过来源 SessionId。
            if (message.SenderUserId != message.ReceiverUserId)
                outboxEvents.Add(RealtimeMessageEventFactory.CreateSenderEchoEvent(createdEvent, message.SenderUserId));

            await outboxWriter.InsertManyAsync(outboxEvents).ConfigureAwait(false);
        }

        // Perf-3：事务内记录 Created 结果到独立账本，解耦幂等性与 messages 行生命周期。
        // 与消息写入同生共死：ledger INSERT 失败则整个事务回滚（更正确的行为）。
        if (_idempotencyLedger is not null)
        {
            await _idempotencyLedger
                .RecordInTransactionAsync(
                    session.Connection,
                    session.Transaction,
                    message.MessageId,
                    message.SenderUserId,
                    message.ClientMessageId,
                    fingerprint,
                    IdempotencyLedgerResultKind.Created,
                    message.MessageId,
                    message.ReceivedAtMs,
                    ct)
                .ConfigureAwait(false);
        }

        await session.CommitAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "实时消息已通过 Npgsql 写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);
        return RealtimeMessagePersistResult.Created(message.MessageId, conversationSequence);
    }

    public async Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：事务内检查接收方生命周期，防止已注销用户更新已读/已送达状态。
        if (!(await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                session.Connection, session.Transaction, session.Schema,
                receipt.ReceiverUserId, ct).ConfigureAwait(false)).IsActive)
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.Unchanged, receipt.MessageId);
        }

        var outboxWriter = new PostgresOutboxWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);

        long senderUserId;
        long receiverUserId;
        long? deliveredAtMs;
        long? readAtMs;
        string? conversationId;
        long messageReceivedAtMs;
        long? messageSequence;

        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, delivered_at_ms, read_at_ms,
                                 conversation_id, received_at_ms, conversation_sequence
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         session.Connection,
                         session.Transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            await using var reader = await command
                .ExecuteReaderAsync(session.CancellationToken)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(session.CancellationToken).ConfigureAwait(false))
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.MessageNotFound,
                    receipt.MessageId);
            }

            senderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            deliveredAtMs = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            readAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
            conversationId = reader.IsDBNull(4) ? null : reader.GetString(4);
            messageReceivedAtMs = reader.GetInt64(5);
            messageSequence = reader.IsDBNull(6) ? null : reader.GetInt64(6);
        }

        if (receiverUserId != receipt.ReceiverUserId)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.ReceiverMismatch,
                receipt.MessageId,
                senderUserId);
        }

        var shouldApply = receipt.ReceiptType switch
        {
            MessageReceiptType.Delivered => deliveredAtMs is null && readAtMs is null,
            MessageReceiptType.Read => readAtMs is null,
            _ => false
        };
        if (!shouldApply)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.Unchanged,
                receipt.MessageId,
                senderUserId);
        }

        var setClause = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms = @occurred_at_ms, delivered_at_ms = COALESCE(delivered_at_ms, @occurred_at_ms)"
            : "delivered_at_ms = @occurred_at_ms";
        var condition = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms IS NULL"
            : "delivered_at_ms IS NULL AND read_at_ms IS NULL";

        await using (var command = new NpgsqlCommand(
                         $"UPDATE {_databaseSchema.MessagesTableSql} SET {setClause} WHERE message_id = @message_id AND receiver_user_id = @receiver_user_id AND {condition}",
                         session.Connection,
                         session.Transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            command.Parameters.AddWithValue("receiver_user_id", receipt.ReceiverUserId);
            command.Parameters.AddWithValue("occurred_at_ms", receipt.OccurredAtMs);
            var affectedRows = await command
                .ExecuteNonQueryAsync(session.CancellationToken)
                .ConfigureAwait(false);
            if (affectedRows == 0)
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId,
                    senderUserId);
            }
        }

        await outboxWriter
            .InsertAsync(RealtimeMessageEventFactory.CopyForReceipt(eventToPublish, senderUserId))
            .ConfigureAwait(false);

        if (receipt.ReceiptType == MessageReceiptType.Read
            && !string.IsNullOrWhiteSpace(conversationId)
            && messageSequence is long targetSequence)
        {
            // Perf-1：使用 O(1) 序列化 MarkRead，消除 COUNT 扫描。
            var readResult = await conversationWriter
                .TryAdvanceReadBySequenceAsync(
                    conversationId,
                    receipt.ReceiverUserId,
                    targetSequence,
                    receipt.MessageId,
                    messageReceivedAtMs)
                .ConfigureAwait(false);

            if (readResult.Advanced)
            {
                var traceParent = RealtimeTraceContext.CaptureTraceParent();
                var traceState = RealtimeTraceContext.CaptureTraceState();
                var unreadEvent = ConversationWriteCommands.CreateUnreadCountChangedEvent(
                    conversationId,
                    receipt.ReceiverUserId,
                    readResult.UnreadCount,
                    receipt.MessageId,
                    messageReceivedAtMs,
                    causeMessageId: receipt.MessageId,
                    receipt.OccurredAtMs,
                    traceParent,
                    traceState);
                await outboxWriter.InsertAsync(unreadEvent).ConfigureAwait(false);
            }
        }

        await session.CommitAsync().ConfigureAwait(false);

        return new MessageReceiptPersistResult(
            MessageReceiptPersistStatus.Applied,
            receipt.MessageId,
            senderUserId);
    }

    public async Task<MessageRecallPersistResult> ApplyRecallAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        long recalledAtMs,
        long maxAgeMs,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(senderUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recalledAtMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAgeMs);

        var payloadFingerprint = MessageMutationWriter.ComputeMutationFingerprint(
            operation: 2,
            messageId,
            content: string.Empty);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：事务内检查发送方生命周期，防止已注销用户撤回消息。
        if (!(await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                session.Connection, session.Transaction, session.Schema,
                senderUserId, ct).ConfigureAwait(false)).IsActive)
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed, messageId);
        }

        var mutationWriter = new MessageMutationWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        var prior = await mutationWriter
            .TryReadMutationRequestAsync(senderUserId, requestId)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 2
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                return new MessageRecallPersistResult(
                    MessageRecallPersistStatus.RequestConflict,
                    messageId);
            }

            await session.CommitAsync().ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageRecallPersistResult(
                    MessageRecallPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    RecalledAtMs: prior.RecalledAtMs)
                : MessageMutationWriter.MapRecallFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        // P0-6：在读取 messages（FOR UPDATE）之前先锁定 message_state 行。
        // 统一锁顺序为 message_state → messages，消除 Recall 与 Reaction 之间的死锁
        // （Recall 持 messages 等 message_state，Reaction 持 message_state 等 messages）。
        // FK 为 DEFERRABLE，若消息不存在此 INSERT 仍成功，后续 ReadMessageForRecallAsync 返回 null 时回滚即可。
        await mutationWriter.EnsureMessageStateLockedAsync(messageId).ConfigureAwait(false);

        var target = await mutationWriter.ReadMessageForRecallAsync(messageId).ConfigureAwait(false);
        if (target is null)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_not_found",
                    conversationId: null,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotFound,
                messageId);
        }

        if (target.SenderUserId != senderUserId)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        // P0-8：群消息撤回还需验证操作者仍是当前群成员，防止离群后修改旧消息。
        var recallAuth = await _mutationPolicy
            .AuthorizeMutationAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                new MessageMutationContext(
                    target.ConversationId,
                    target.SenderUserId,
                    target.ReceiverUserId,
                    senderUserId,
                    MessageMutationOperation.Recall),
                ct)
            .ConfigureAwait(false);
        if (!recallAuth.Allowed)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (target.RecalledAtMs is long already)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: already)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                already);
        }

        if (recalledAtMs - target.ReceivedAtMs > maxAgeMs)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "recall_window_expired",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.WindowExpired,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        var affected = await mutationWriter
            .ApplyRecallUpdateAsync(messageId, recalledAtMs)
            .ConfigureAwait(false);
        if (affected == 0)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 2,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: recalledAtMs)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageRecallPersistResult(
                MessageRecallPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                recalledAtMs);
        }

        var tipPreviewUpdated = false;
        if (!string.IsNullOrWhiteSpace(target.ConversationId))
        {
            tipPreviewUpdated = await conversationWriter
                .TryUpdateConversationTipPreviewAsync(
                    target.ConversationId,
                    messageId,
                    "消息已撤回")
                .ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var isGroup = !string.IsNullOrWhiteSpace(target.ConversationId)
                      && ConversationId.IsGroup(target.ConversationId);

        if (isGroup)
        {
            // Perf-9：群撤回走统一 GroupProjectionDelta 协议，广播事件聚合为单行 Outbox。
            var memberIds = await conversationWriter
                .ListActiveMemberUserIdsAsync(target.ConversationId!)
                .ConfigureAwait(false);
            var delta = new GroupProjectionDelta(target.ConversationId!, memberIds);

            delta.AddBroadcast(GroupProjectionEventFactory.CreateGroupMessageRecalledBroadcast(
                messageId,
                target.ConversationId!,
                senderUserId,
                target.ReceiverUserId,
                senderSessionId,
                recalledAtMs,
                target.ConversationSequence,
                traceParent,
                traceState));

            if (tipPreviewUpdated)
            {
                var cause = $"recall:{recalledAtMs}";
                delta.AddBroadcast(GroupProjectionEventFactory.CreateGroupConversationChangedBroadcast(
                    target.ConversationId!,
                    messageId,
                    "消息已撤回",
                    target.ReceivedAtMs,
                    senderUserId,
                    cause,
                    traceParent,
                    traceState));
            }

            await outboxWriter.InsertManyAsync(delta.Build()).ConfigureAwait(false);
        }
        else
        {
            var payloadJson = JsonSerializer.Serialize(
                new RealtimeMessageRecalledPayload
                {
                    MessageId = messageId,
                    ConversationId = target.ConversationId,
                    SenderUserId = senderUserId,
                    ReceiverUserId = target.ReceiverUserId,
                    RecalledAtMs = recalledAtMs,
                    ConversationSequence = target.ConversationSequence
                },
                RealtimeJsonSerializerContext.Default.RealtimeMessageRecalledPayload);

            var events = new List<RealtimeEvent>(8);
            events.Add(new RealtimeEvent
            {
                EventId = MessageEventIdFactory.CreateMessageRecalledEventId(messageId, target.ReceiverUserId),
                Type = RealtimeEventType.MessageRecalled,
                TargetUserId = target.ReceiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = recalledAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != target.ReceiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageRecalledEventId(messageId, senderUserId),
                    Type = RealtimeEventType.MessageRecalled,
                    TargetUserId = senderUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = recalledAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(target.ConversationId))
            {
                var cause = $"recall:{recalledAtMs}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    senderUserId,
                    target.ReceiverUserId,
                    messageId,
                    "消息已撤回",
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    target.ReceiverUserId,
                    senderUserId,
                    messageId,
                    "消息已撤回",
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }

            await outboxWriter.InsertManyAsync(events).ConfigureAwait(false);
        }
        await mutationWriter.InsertMutationRequestAsync(
                senderUserId,
                requestId,
                operation: 2,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                target.ConversationId,
                content: null,
                editVersion: null,
                editedAtMs: null,
                recalledAtMs: recalledAtMs)
            .ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);

        return new MessageRecallPersistResult(
            MessageRecallPersistStatus.Applied,
            messageId,
            target.ReceiverUserId,
            target.ConversationId,
            recalledAtMs);
    }

    public async Task<MessageEditPersistResult> ApplyEditAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        string content,
        long editedAtMs,
        long maxAgeMs,
        IReadOnlyList<long>? mentionedUserIds = null,
        IReadOnlyList<string>? mentionedRoles = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentNullException.ThrowIfNull(content);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(senderUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(editedAtMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAgeMs);

        var payloadFingerprint = MessageMutationWriter.ComputeMutationFingerprint(
            operation: 1,
            messageId,
            content,
            mentionedUserIds,
            mentionedRoles);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);

        // P0-2：事务内检查发送方生命周期，防止已注销用户编辑消息。
        if (!(await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                session.Connection, session.Transaction, session.Schema,
                senderUserId, ct).ConfigureAwait(false)).IsActive)
        {
            await session.RollbackAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed, messageId);
        }

        var mutationWriter = new MessageMutationWriter(session);
        var conversationWriter = new ConversationProjectionWriter(session);
        var outboxWriter = new PostgresOutboxWriter(session);

        var prior = await mutationWriter
            .TryReadMutationRequestAsync(senderUserId, requestId)
            .ConfigureAwait(false);
        if (prior is not null)
        {
            if (prior.Operation != 1
                || !string.Equals(prior.MessageId, messageId, StringComparison.Ordinal)
                || !string.Equals(prior.PayloadFingerprint, payloadFingerprint, StringComparison.Ordinal))
            {
                await session.RollbackAsync().ConfigureAwait(false);
                return new MessageEditPersistResult(
                    MessageEditPersistStatus.RequestConflict,
                    messageId);
            }

            await session.CommitAsync().ConfigureAwait(false);
            return prior.Succeeded
                ? new MessageEditPersistResult(
                    MessageEditPersistStatus.Unchanged,
                    messageId,
                    ConversationId: prior.ConversationId,
                    Content: prior.Content,
                    EditVersion: prior.EditVersion,
                    EditedAtMs: prior.EditedAtMs)
                : MessageMutationWriter.MapEditFailure(prior.ErrorCode, messageId, prior.ConversationId);
        }

        var target = await mutationWriter.ReadMessageForEditAsync(messageId).ConfigureAwait(false);
        if (target is null)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_not_found",
                    conversationId: null,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotFound,
                messageId);
        }

        if (target.SenderUserId != senderUserId)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        // P0-8：群消息编辑还需验证操作者仍是当前群成员，防止离群后修改旧消息。
        var editAuth = await _mutationPolicy
            .AuthorizeMutationAsync(
                session.Connection,
                session.Transaction,
                session.Schema,
                new MessageMutationContext(
                    target.ConversationId,
                    target.SenderUserId,
                    target.ReceiverUserId,
                    senderUserId,
                    MessageMutationOperation.Edit),
                ct)
            .ConfigureAwait(false);
        if (!editAuth.Allowed)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_not_allowed",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.NotAllowed,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (target.RecalledAtMs is not null)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_recalled",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: target.RecalledAtMs)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (editedAtMs - target.ReceivedAtMs > maxAgeMs)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "edit_window_expired",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.WindowExpired,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        if (string.Equals(target.Content, content, StringComparison.Ordinal))
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: true,
                    errorCode: null,
                    target.ConversationId,
                    content: target.Content,
                    editVersion: target.EditVersion,
                    editedAtMs: editedAtMs,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.Unchanged,
                messageId,
                target.ReceiverUserId,
                target.ConversationId,
                target.Content,
                target.EditVersion,
                editedAtMs);
        }

        var nextVersion = target.EditVersion + 1;

        // Mentions 业务闭环：编辑路径的 mention 规范化。
        // null 表示不修改现有 mentions（COALESCE 保留原值）；非空数组经过 MentionValidator
        // 规范化后替换原值。规范化逻辑与新增消息一致：
        // 去重 / 排除自身 / 截断 / 群成员校验 / @all|@admin 权限校验。
        var hasMentionChanges = mentionedUserIds is not null || mentionedRoles is not null;
        long[]? dbMentionUserIds = null;
        string[]? dbMentionRoles = null;
        IReadOnlyList<long>? payloadMentionUserIds = null;
        IReadOnlyList<string>? payloadMentionRoles = null;
        IReadOnlyList<long>? groupMemberIds = null;

        var isGroup = !string.IsNullOrWhiteSpace(target.ConversationId)
                      && ConversationId.IsGroup(target.ConversationId);

        if (hasMentionChanges && isGroup)
        {
            groupMemberIds = await conversationWriter
                .ListActiveMemberUserIdsAsync(target.ConversationId!)
                .ConfigureAwait(false);
            var senderRole = await conversationWriter
                .TryGetMemberRoleAsync(target.ConversationId!, senderUserId)
                .ConfigureAwait(false);
            var isManager = senderRole == (short)ConversationMemberRole.Owner
                            || senderRole == (short)ConversationMemberRole.Admin;
            var memberSet = new HashSet<long>(groupMemberIds);
            dbMentionUserIds = mentionedUserIds is not null
                ? MentionValidator.NormalizeUserIds(mentionedUserIds, senderUserId, memberSet)
                : null;
            dbMentionRoles = mentionedRoles is not null
                ? MentionValidator.NormalizeRoles(mentionedRoles, isManager)
                : null;
            payloadMentionUserIds = MentionValidator.AsReadOnly(dbMentionUserIds);
            payloadMentionRoles = MentionValidator.AsReadOnly(dbMentionRoles);
        }
        else if (hasMentionChanges)
        {
            // 单聊：基本去重 + 排除自身；@all/@admin 无意义但允许透传（isManager=true）。
            dbMentionUserIds = mentionedUserIds is not null
                ? MentionValidator.NormalizeUserIds(mentionedUserIds, senderUserId)
                : null;
            dbMentionRoles = mentionedRoles is not null
                ? MentionValidator.NormalizeRoles(mentionedRoles, isManager: true)
                : null;
            payloadMentionUserIds = MentionValidator.AsReadOnly(dbMentionUserIds);
            payloadMentionRoles = MentionValidator.AsReadOnly(dbMentionRoles);
        }

        var updateResult = await mutationWriter
            .ApplyEditUpdateAsync(
                messageId,
                content,
                nextVersion,
                editedAtMs,
                dbMentionUserIds,
                dbMentionRoles,
                target.MentionedUserIds)
            .ConfigureAwait(false);
        var affected = updateResult.Affected;
        // 一-4：本次 Edit 的 mention 增减 diff，供 MessageEditPersistResult 与事件 payload 携带。
        var addedMentionedUserIds = updateResult.AddedMentionedUserIds;
        var removedMentionedUserIds = updateResult.RemovedMentionedUserIds;
        if (affected == 0)
        {
            await mutationWriter.InsertMutationRequestAsync(
                    senderUserId,
                    requestId,
                    operation: 1,
                    messageId,
                    payloadFingerprint,
                    succeeded: false,
                    errorCode: "message_recalled",
                    target.ConversationId,
                    content: null,
                    editVersion: null,
                    editedAtMs: null,
                    recalledAtMs: null)
                .ConfigureAwait(false);
            await session.CommitAsync().ConfigureAwait(false);
            return new MessageEditPersistResult(
                MessageEditPersistStatus.AlreadyRecalled,
                messageId,
                target.ReceiverUserId,
                target.ConversationId);
        }

        var tipPreviewUpdated = false;
        var tipPreview = ConversationId.CreatePreview(content);
        if (!string.IsNullOrWhiteSpace(target.ConversationId))
        {
            tipPreviewUpdated = await conversationWriter
                .TryUpdateConversationTipPreviewAsync(
                    target.ConversationId,
                    messageId,
                    tipPreview)
                .ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        if (isGroup)
        {
            // Perf-9：群编辑走统一 GroupProjectionDelta 协议，广播事件聚合为单行 Outbox。
            // 复用 mention 规范化阶段已加载的成员列表（若未加载则在此加载）。
            if (groupMemberIds is null)
            {
                groupMemberIds = await conversationWriter
                    .ListActiveMemberUserIdsAsync(target.ConversationId!)
                    .ConfigureAwait(false);
            }
            var delta = new GroupProjectionDelta(target.ConversationId!, groupMemberIds);

            delta.AddBroadcast(GroupProjectionEventFactory.CreateGroupMessageEditedBroadcast(
                messageId,
                target.ConversationId!,
                senderUserId,
                target.ReceiverUserId,
                senderSessionId,
                content,
                nextVersion,
                editedAtMs,
                payloadMentionUserIds,
                payloadMentionRoles,
                target.ConversationSequence,
                traceParent,
                traceState));

            if (tipPreviewUpdated)
            {
                var cause = $"edit:{nextVersion}";
                delta.AddBroadcast(GroupProjectionEventFactory.CreateGroupConversationChangedBroadcast(
                    target.ConversationId!,
                    messageId,
                    tipPreview,
                    target.ReceivedAtMs,
                    senderUserId,
                    cause,
                    traceParent,
                    traceState));
            }

            await outboxWriter.InsertManyAsync(delta.Build()).ConfigureAwait(false);
        }
        else
        {
            var payloadJson = JsonSerializer.Serialize(
                new RealtimeMessageEditedPayload
                {
                    MessageId = messageId,
                    ConversationId = target.ConversationId,
                    SenderUserId = senderUserId,
                    ReceiverUserId = target.ReceiverUserId,
                    Content = content,
                    EditVersion = nextVersion,
                    EditedAtMs = editedAtMs,
                    ConversationSequence = target.ConversationSequence,
                    MentionedUserIds = payloadMentionUserIds,
                    MentionedRoles = payloadMentionRoles,
                    AddedMentionedUserIds = addedMentionedUserIds,
                    RemovedMentionedUserIds = removedMentionedUserIds
                },
                RealtimeJsonSerializerContext.Default.RealtimeMessageEditedPayload);

            var events = new List<RealtimeEvent>(8);
            events.Add(new RealtimeEvent
            {
                EventId = MessageEventIdFactory.CreateMessageEditedEventId(
                    messageId,
                    target.ReceiverUserId,
                    nextVersion),
                Type = RealtimeEventType.MessageEdited,
                TargetUserId = target.ReceiverUserId,
                ActorUserId = senderUserId,
                MessageId = messageId,
                SessionId = senderSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = editedAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
            if (senderUserId != target.ReceiverUserId)
            {
                events.Add(new RealtimeEvent
                {
                    EventId = MessageEventIdFactory.CreateMessageEditedEventId(
                        messageId,
                        senderUserId,
                        nextVersion),
                    Type = RealtimeEventType.MessageEdited,
                    TargetUserId = senderUserId,
                    ActorUserId = senderUserId,
                    MessageId = messageId,
                    SessionId = senderSessionId,
                    PayloadJson = payloadJson,
                    OccurredAtMs = editedAtMs,
                    TraceParent = traceParent,
                    TraceState = traceState
                });
            }

            if (tipPreviewUpdated && !string.IsNullOrWhiteSpace(target.ConversationId))
            {
                var cause = $"edit:{nextVersion}";
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    senderUserId,
                    target.ReceiverUserId,
                    messageId,
                    tipPreview,
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
                events.Add(ConversationWriteCommands.CreateConversationChangedEvent(
                    target.ConversationId,
                    target.ReceiverUserId,
                    senderUserId,
                    messageId,
                    tipPreview,
                    target.ReceivedAtMs,
                    senderUserId,
                    traceParent,
                    traceState,
                    cause));
            }

            await outboxWriter.InsertManyAsync(events).ConfigureAwait(false);
        }
        await mutationWriter.InsertMutationRequestAsync(
                senderUserId,
                requestId,
                operation: 1,
                messageId,
                payloadFingerprint,
                succeeded: true,
                errorCode: null,
                target.ConversationId,
                content: content,
                editVersion: nextVersion,
                editedAtMs: editedAtMs,
                recalledAtMs: null)
            .ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);

        return new MessageEditPersistResult(
            MessageEditPersistStatus.Applied,
            messageId,
            target.ReceiverUserId,
            target.ConversationId,
            content,
            nextVersion,
            editedAtMs)
        {
            AddedMentionedUserIds = addedMentionedUserIds,
            RemovedMentionedUserIds = removedMentionedUserIds
        };
    }

    public async Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 5_000);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        long total = 0;
        while (true)
        {
            await using var command = new NpgsqlCommand(
                $"""
                 DELETE FROM {_databaseSchema.MessagesTableSql}
                 WHERE ctid IN (
                     SELECT ctid FROM {_databaseSchema.MessagesTableSql}
                     WHERE sender_user_id = @user_id OR receiver_user_id = @user_id
                     LIMIT @batch_size
                 );
                 """,
                connection);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("batch_size", batchSize);
            var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (deleted <= 0)
                break;
            total += deleted;
        }

        // 六-4：清理其他消息中 @ 提及该用户的 mentioned_user_ids 数组元素。
        // 该用户自己的消息已在上方删除，此处仅影响仍存活的消息。
        await using (var mentionedCleanupCmd = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.MessagesTableSql}
                          SET mentioned_user_ids = array_remove(mentioned_user_ids, @user_id)
                          WHERE @user_id = ANY(mentioned_user_ids);
                          """,
                         connection))
        {
            mentionedCleanupCmd.Parameters.AddWithValue("user_id", userId);
            await mentionedCleanupCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 六-4：清理该用户发起的群操作幂等账本（group_mutation_requests）。
        // group_operation_audit 与 tombstone/idempotency_ledger 有意保留（审计 / 防 replay）。
        await using (var groupMutationCleanupCmd = new NpgsqlCommand(
                         $"""
                          DELETE FROM {_databaseSchema.GroupMutationRequestsTableSql}
                          WHERE actor_user_id = @user_id;
                          """,
                         connection))
        {
            groupMutationCleanupCmd.Parameters.AddWithValue("user_id", userId);
            await groupMutationCleanupCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await DeleteConversationDataForUserIfPresentAsync(connection, userId, ct)
            .ConfigureAwait(false);

        var outboxDeleted = 0;
        await using (var outboxDeleteCmd = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE target_user_id = @user_id
               AND (target_user_ids IS NULL OR cardinality(target_user_ids) = 0)
               AND event_type <> ALL(@keep_types);
             """,
            connection))
        {
            outboxDeleteCmd.Parameters.AddWithValue("user_id", userId);
            outboxDeleteCmd.Parameters.AddWithValue(
                "keep_types",
                new short[]
                {
                    (short)RealtimeEventType.AccountCleanupCompleted,
                    (short)RealtimeEventType.AttachmentBlobsPurge
                });
            outboxDeleted = await outboxDeleteCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 四-1：账号删除只需集合化修改 target_user_ids 列，不再逐行反序列化和重写 JSON。
        // 因为 payload_utf8 不再包含 TargetUserIds（OutboxInsertHelper 已排除），所以无需重写 payload。
        // 旧记录（payload_utf8 仍含 TargetUserIds）的 Publisher 会从列读取路由，不从 payload 读取。
        var keepTypes = new short[]
        {
            (short)RealtimeEventType.AccountCleanupCompleted,
            (short)RealtimeEventType.AttachmentBlobsPurge
        };

        // 第一步：删除 target_user_ids 变为空数组的行（整行不再有目标用户）。
        await using var deleteEmptyCmd = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE @user_id = ANY(target_user_ids)
               AND cardinality(array_remove(target_user_ids, @user_id)) = 0
               AND status <> @published_status
               AND event_type <> ALL(@keep_types);
             """,
            connection);
        deleteEmptyCmd.Parameters.AddWithValue("user_id", userId);
        deleteEmptyCmd.Parameters.AddWithValue(
            "published_status",
            (short)RealtimeOutboxStatus.Published);
        deleteEmptyCmd.Parameters.AddWithValue("keep_types", keepTypes);
        outboxDeleted += await deleteEmptyCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // 第二步：对仍有其他用户的行做 array_remove。
        await using var removeCmd = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.OutboxTableSql}
             SET target_user_ids = array_remove(target_user_ids, @user_id)
             WHERE @user_id = ANY(target_user_ids)
               AND status <> @published_status
               AND event_type <> ALL(@keep_types);
             """,
            connection);
        removeCmd.Parameters.AddWithValue("user_id", userId);
        removeCmd.Parameters.AddWithValue(
            "published_status",
            (short)RealtimeOutboxStatus.Published);
        removeCmd.Parameters.AddWithValue("keep_types", keepTypes);
        outboxDeleted += await removeCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (total > 0 || outboxDeleted > 0)
        {
            _logger.LogInformation(
                "已清理用户消息与 Outbox。用户={UserId}；删除消息={Deleted}；删除Outbox={OutboxDeleted}",
                userId,
                total,
                outboxDeleted);
        }

        return total;
    }

    private async Task DeleteConversationDataForUserIfPresentAsync(
        NpgsqlConnection connection,
        long userId,
        CancellationToken ct)
    {
        await using (var existsCmd = new NpgsqlCommand(
                           """
                           SELECT to_regclass(@qualified) IS NOT NULL;
                           """,
                           connection))
        {
            existsCmd.Parameters.AddWithValue(
                "qualified",
                $"{_databaseSchema.Schema}.conversation_members");
            var exists = await existsCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (exists is not true)
                return;
        }

        // Direct：删除双方会话（消息已按 sender/receiver 清掉，留下空壳无意义）。
        // 非 Direct：墓碑删除成员，并清理仍指向已删用户的 tip / 未读。
        await using (var directCleanup = new NpgsqlCommand(
                           $"""
                            WITH user_direct AS (
                                SELECT m.conversation_id
                                FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                INNER JOIN {_databaseSchema.ConversationsTableSql} AS c
                                    ON c.conversation_id = m.conversation_id
                                WHERE m.user_id = @user_id
                                  AND c.type = @direct_type
                            ),
                            delete_direct_members AS (
                                DELETE FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                USING user_direct d
                                WHERE m.conversation_id = d.conversation_id
                                RETURNING m.conversation_id
                            )
                            DELETE FROM {_databaseSchema.ConversationsTableSql} AS c
                            USING user_direct d
                            WHERE c.conversation_id = d.conversation_id;
                            """,
                           connection))
        {
            directCleanup.Parameters.AddWithValue("user_id", userId);
            directCleanup.Parameters.AddWithValue("direct_type", (short)ConversationType.Direct);
            await directCleanup.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var affectedConversationIds = new HashSet<string>(StringComparer.Ordinal);

        await using (var tombstone = new NpgsqlCommand(
                           $"""
                            WITH removed AS (
                                DELETE FROM {_databaseSchema.ConversationMembersTableSql}
                                WHERE user_id = @user_id
                                RETURNING conversation_id
                            ),
                            fix_peer AS (
                                UPDATE {_databaseSchema.ConversationMembersTableSql} AS m
                                SET peer_user_id = CASE WHEN m.peer_user_id = @user_id THEN NULL ELSE m.peer_user_id END
                                FROM removed r
                                WHERE m.conversation_id = r.conversation_id
                                  AND m.user_id <> @user_id
                                RETURNING m.conversation_id
                            )
                            SELECT conversation_id FROM removed
                            UNION
                            SELECT conversation_id FROM fix_peer;
                            """,
                           connection))
        {
            tombstone.Parameters.AddWithValue("user_id", userId);
            await using var reader = await tombstone.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var conversationId = reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(conversationId))
                    affectedConversationIds.Add(conversationId);
            }
        }

        // P0-5：使用统一的 ConversationProjectionRepair 修复 tip 和未读数，
        // 而非粗暴地将 unread_count 归零、清空 last_read 和 tip。
        // 修复逻辑与 Retention 完全一致：DISTINCT ON 找剩余最新消息，
        // 依据成员原 last_read 水位重新计算 unread_count。
        if (affectedConversationIds.Count > 0)
        {
            await ConversationProjectionRepair.RepairConversationTipsAsync(
                connection, transaction: null, _databaseSchema, affectedConversationIds, ct)
                .ConfigureAwait(false);
            await ConversationProjectionRepair.RepairUnreadCountsAsync(
                connection, transaction: null, _databaseSchema, affectedConversationIds, ct)
                .ConfigureAwait(false);
        }

        // 五-2：匿名化群聊会话的 created_by_user_id。
        // 单聊会话已在上方被直接删除，此处仅影响群聊（type = Group）。
        // 将已删除用户创建的群聊的 created_by_user_id 设为 NULL，表示创建者已注销。
        // 保留会话本身（仍有其他成员），仅清除创建者身份引用。
        await using (var anonymizeCreatedByCmd = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationsTableSql}
                          SET created_by_user_id = NULL
                          WHERE created_by_user_id = @user_id
                            AND type <> @direct_type;
                          """,
                         connection))
        {
            anonymizeCreatedByCmd.Parameters.AddWithValue("user_id", userId);
            anonymizeCreatedByCmd.Parameters.AddWithValue("direct_type", (short)ConversationType.Direct);
            await anonymizeCreatedByCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var orphanConversationsCmd = new NpgsqlCommand(
                           $"""
                            DELETE FROM {_databaseSchema.ConversationsTableSql} AS c
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {_databaseSchema.ConversationMembersTableSql} AS m
                                WHERE m.conversation_id = c.conversation_id
                            );
                            """,
                           connection))
        {
            await orphanConversationsCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 一-1：查询 Reply 源消息的权威 snapshot。
    /// </summary>
    public async Task<(long SenderUserId, string Preview)?> GetReplySourceAsync(
        string messageId,
        string conversationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT sender_user_id,
                    CASE
                        WHEN recalled_at_ms IS NOT NULL THEN NULL
                        WHEN LENGTH(content) > 256 THEN LEFT(content, 256)
                        ELSE content
                    END AS preview
             FROM {_databaseSchema.MessagesTableSql}
             WHERE message_id = @message_id
               AND conversation_id = @conversation_id
               AND recalled_at_ms IS NULL;
             """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("conversation_id", conversationId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var senderUserId = reader.GetInt64(0);
        if (reader.IsDBNull(1))
            return null; // 源消息已撤回（recalled_at_ms IS NOT NULL 时 preview 为 NULL）

        var preview = reader.GetString(1);
        return (senderUserId, preview);
    }

    /// <summary>
    /// 一-3：批量查询已被撤回的消息编号。
    /// </summary>
    public async Task<IReadOnlyList<string>> BatchGetRecalledMessageIdsAsync(
        IReadOnlyCollection<string> messageIds,
        CancellationToken cancellationToken = default)
    {
        if (messageIds is null || messageIds.Count == 0)
            return Array.Empty<string>();

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        var ids = messageIds.ToArray();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT message_id
             FROM {_databaseSchema.MessagesTableSql}
             WHERE recalled_at_ms IS NOT NULL
               AND message_id = ANY(@message_ids);
             """,
            connection);
        command.Parameters.AddWithValue("message_ids", ids);

        var result = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    public async Task EnqueueEventAsync(RealtimeEvent eventToPublish, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventToPublish);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventToPublish.EventId);

        await using var session = await _sessionFactory.BeginAsync(ct).ConfigureAwait(false);
        var outboxWriter = new PostgresOutboxWriter(session);
        await outboxWriter.InsertAsync(eventToPublish).ConfigureAwait(false);
        await session.CommitAsync().ConfigureAwait(false);
    }
}

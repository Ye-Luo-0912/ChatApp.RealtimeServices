using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;
using ChatApp.Realtime.Infrastructure.Postgres.Messages;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class EfCoreRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly IDbContextFactory<RealtimeDbContext> _dbContextFactory;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly ILogger<EfCoreRealtimeMessageStore> _logger;

    public EfCoreRealtimeMessageStore(
        IDbContextFactory<RealtimeDbContext> dbContextFactory,
        RealtimeDatabaseSchema databaseSchema,
        ILogger<EfCoreRealtimeMessageStore> logger)
    {
        _dbContextFactory = dbContextFactory;
        _databaseSchema = databaseSchema;
        _logger = logger;
    }

    public async Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        var fingerprint = RealtimeMessageFingerprint.Compute(
            message.ReceiverUserId,
            message.Content,
            message.AttachmentIds);

        // P1-4：EfCore 路径不绑定附件，但仍需要把应用层传入的 Payload 对象物化为 PayloadJson，
        // 否则 Outbox 行会缺少 payload。附件参数为 null，保留原 payload 字段不变。
        // 若 eventToPublish 已有 PayloadJson（旧调用方/测试），EnrichChatMessagePayload 也能处理。
        eventToPublish = RealtimeMessageEventFactory.EnrichChatMessagePayload(eventToPublish, attachments: null);

        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);

        await using var transaction = await dbContext.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

        var existing = await dbContext.Messages
            .Where(m => m.SenderUserId == message.SenderUserId
                        && m.ClientMessageId == message.ClientMessageId)
            .Select(m => new
            {
                m.MessageId,
                m.ReceiverUserId,
                m.Content,
                m.ContentFingerprint
            })
            .SingleOrDefaultAsync(ct)
            .ConfigureAwait(false);

        if (existing is not null)
        {
            // EfCore 路径不绑定附件；冲突比较时已有附件视为空集（生产路径为 Npgsql）。
            if (!RealtimeMessageFingerprint.MatchesExisting(
                    existing.ContentFingerprint,
                    existing.ReceiverUserId,
                    existing.Content,
                    Array.Empty<string>(),
                    fingerprint))
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                _logger.LogWarning(
                    "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                    message.ClientMessageId,
                    message.SenderUserId,
                    existing.MessageId);
                return RealtimeMessagePersistResult.Conflict(existing.MessageId);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            // 消息与首条 Outbox 同 SaveChanges：重复投递不重建已 Published/Dead（或已清理）的 Outbox。
            _logger.LogDebug(
                "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return RealtimeMessagePersistResult.Duplicate(existing.MessageId);
        }

        dbContext.Messages.Add(new RealtimeMessageEntity
        {
            MessageId = message.MessageId,
            ClientMessageId = message.ClientMessageId,
            SenderUserId = message.SenderUserId,
            SenderSessionId = message.SenderSessionId,
            ReceiverUserId = message.ReceiverUserId,
            ConversationId = message.ConversationId,
            Content = message.Content,
            ContentFingerprint = fingerprint,
            ReceivedAtMs = message.ReceivedAtMs,
            ReplyToMessageId = message.ReplyToMessageId,
            ReplyToSenderUserId = message.ReplyToSenderUserId,
            ReplyToPreview = message.ReplyToPreview,
            ForwardedFromMessageId = message.ForwardedFromMessageId,
            ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
            ForwardedFromPreview = message.ForwardedFromPreview,
            MentionedUserIds = message.MentionedUserIds,
            MentionedRoles = message.MentionedRoles
        });
        dbContext.Outbox.Add(CreateOutboxEntity(eventToPublish, message.MessageId));

        try
        {
            await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);

            if (message.SenderUserId != message.ReceiverUserId)
            {
                dbContext.Outbox.Add(CreateOutboxEntity(
                    CreateSenderEchoEvent(
                        CopyWithMessageId(eventToPublish, message.MessageId),
                        message.SenderUserId)));
            }

            if (!string.IsNullOrWhiteSpace(message.ConversationId))
            {
                await AdvanceConversationAndEnqueueAsync(
                        dbContext,
                        message,
                        eventToPublish.TraceParent,
                        eventToPublish.TraceState,
                        ct)
                    .ConfigureAwait(false);
                await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateException ex)
            when (ex.InnerException is PostgresException pgEx
                  && pgEx.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            dbContext.ChangeTracker.Clear();
            var concurrent = await dbContext.Messages
                .Where(m => m.SenderUserId == message.SenderUserId
                            && m.ClientMessageId == message.ClientMessageId)
                .Select(m => new
                {
                    m.MessageId,
                    m.ReceiverUserId,
                    m.Content,
                    m.ContentFingerprint
                })
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);
            if (concurrent is null)
                throw;

            // EfCore 路径不绑定附件；冲突比较时已有附件视为空集（生产路径为 Npgsql）。
            if (!RealtimeMessageFingerprint.MatchesExisting(
                    concurrent.ContentFingerprint,
                    concurrent.ReceiverUserId,
                    concurrent.Content,
                    Array.Empty<string>(),
                    fingerprint))
            {
                _logger.LogWarning(
                    "入站消息幂等键内容冲突（并发写入检测）。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                    message.ClientMessageId,
                    message.SenderUserId);
                return RealtimeMessagePersistResult.Conflict(concurrent.MessageId);
            }

            _logger.LogDebug(
                "实时消息存在（并发写入检测），跳过重复。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return RealtimeMessagePersistResult.Duplicate(concurrent.MessageId);
        }

        _logger.LogDebug(
            "实时消息已写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        return RealtimeMessagePersistResult.Created(message.MessageId);
    }

    public async Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await dbContext.Database
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var message = await dbContext.Messages
            .SingleOrDefaultAsync(
                item => item.MessageId == receipt.MessageId,
                ct)
            .ConfigureAwait(false);
        if (message is null)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.MessageNotFound,
                receipt.MessageId);
        }

        if (message.ReceiverUserId != receipt.ReceiverUserId)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.ReceiverMismatch,
                receipt.MessageId,
                message.SenderUserId);
        }

        var shouldApply = receipt.ReceiptType switch
        {
            MessageReceiptType.Delivered =>
                message.DeliveredAtMs is null && message.ReadAtMs is null,
            MessageReceiptType.Read => message.ReadAtMs is null,
            _ => false
        };
        if (!shouldApply)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.Unchanged,
                receipt.MessageId,
                message.SenderUserId);
        }

        if (receipt.ReceiptType == MessageReceiptType.Read)
        {
            message.ReadAtMs = receipt.OccurredAtMs;
            message.DeliveredAtMs ??= receipt.OccurredAtMs;
        }
        else
        {
            message.DeliveredAtMs = receipt.OccurredAtMs;
        }

        dbContext.Outbox.Add(
            CreateReceiptOutboxEntity(
                eventToPublish,
                message.SenderUserId));
        await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        return new MessageReceiptPersistResult(
            MessageReceiptPersistStatus.Applied,
            receipt.MessageId,
            message.SenderUserId);
    }

    private async Task AdvanceConversationAndEnqueueAsync(
        RealtimeDbContext dbContext,
        RealtimeMessageRecord message,
        string? traceParent,
        string? traceState,
        CancellationToken ct)
    {
        var connection = (NpgsqlConnection)dbContext.Database.GetDbConnection();
        var dbTransaction = dbContext.Database.CurrentTransaction
            ?? throw new InvalidOperationException("会话投影写入必须在事务内执行。");
        var transaction = (NpgsqlTransaction)dbTransaction.GetDbTransaction();

        var conversationId = message.ConversationId!;
        var preview = ConversationId.CreatePreview(message.Content);
        var (advanced, unread) = await ConversationWriteCommands.TryAdvanceAndIncrementUnreadAsync(
                connection,
                transaction,
                _databaseSchema,
                conversationId,
                message.SenderUserId,
                message.ReceiverUserId,
                message.MessageId,
                preview,
                message.ReceivedAtMs,
                ct)
            .ConfigureAwait(false);

        if (advanced)
        {
            dbContext.Outbox.Add(CreateOutboxEntity(
                ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    message.SenderUserId,
                    message.ReceiverUserId,
                    message.MessageId,
                    preview,
                    message.ReceivedAtMs,
                    message.SenderUserId,
                    traceParent,
                    traceState)));
            dbContext.Outbox.Add(CreateOutboxEntity(
                ConversationWriteCommands.CreateConversationChangedEvent(
                    conversationId,
                    message.ReceiverUserId,
                    message.SenderUserId,
                    message.MessageId,
                    preview,
                    message.ReceivedAtMs,
                    message.SenderUserId,
                    traceParent,
                    traceState)));
        }

        if (unread is int unreadCount)
        {
            dbContext.Outbox.Add(CreateOutboxEntity(
                ConversationWriteCommands.CreateUnreadCountChangedEvent(
                    conversationId,
                    message.ReceiverUserId,
                    unreadCount,
                    lastReadMessageId: null,
                    lastReadAtMs: null,
                    causeMessageId: message.MessageId,
                    message.ReceivedAtMs,
                    traceParent,
                    traceState)));
        }
    }

    private static RealtimeEvent CopyWithMessageId(RealtimeEvent evt, string messageId) =>
        new()
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = messageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };

    private static RealtimeEvent CreateSenderEchoEvent(RealtimeEvent receiverEvent, long senderUserId)
    {
        var messageId = receiverEvent.MessageId ?? string.Empty;
        return new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateSenderEchoEventId(messageId, senderUserId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = senderUserId,
            ActorUserId = receiverEvent.ActorUserId,
            MessageId = receiverEvent.MessageId,
            SessionId = receiverEvent.SessionId,
            PayloadJson = receiverEvent.PayloadJson,
            OccurredAtMs = receiverEvent.OccurredAtMs,
            TraceParent = receiverEvent.TraceParent,
            TraceState = receiverEvent.TraceState
        };
    }

    private static RealtimeOutboxEntity CreateReceiptOutboxEntity(
        RealtimeEvent evt,
        long senderUserId)
    {
        var persistedEvent = new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = senderUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };

        return CreateOutboxEntity(persistedEvent);
    }

    private static RealtimeOutboxEntity CreateOutboxEntity(RealtimeEvent evt, string messageId)
    {
        var persistedEvent = new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = messageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };

        return CreateOutboxEntity(persistedEvent);
    }

    private static RealtimeOutboxEntity CreateOutboxEntity(RealtimeEvent evt) =>
        new()
        {
            EventId = evt.EventId,
            PayloadJson = JsonSerializer.Serialize(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent),
            TargetUserId = evt.TargetUserId,
            EventType = (short)evt.Type,
            Status = (short)RealtimeOutboxStatus.Pending
        };

    public Task<MessageRecallPersistResult> ApplyRecallAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        long recalledAtMs,
        long maxAgeMs,
        CancellationToken ct = default) =>
        throw new NotSupportedException(
            "EfCoreRealtimeMessageStore 不支持撤回；请使用 NpgsqlRealtimeMessageStore。");

    public Task<MessageEditPersistResult> ApplyEditAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        string content,
        long editedAtMs,
        long maxAgeMs,
        CancellationToken ct = default) =>
        throw new NotSupportedException(
            "EfCoreRealtimeMessageStore 不支持编辑；请使用 NpgsqlRealtimeMessageStore。");

    public async Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 5_000);
        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);

        long total = 0;
        while (true)
        {
            var batch = await dbContext.Messages
                .Where(m => m.SenderUserId == userId || m.ReceiverUserId == userId)
                .OrderBy(m => m.MessageId)
                .Take(batchSize)
                .ToListAsync(ct)
                .ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            dbContext.Messages.RemoveRange(batch);
            await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
            total += batch.Count;
        }

        var keepTypes = new HashSet<short>
        {
            (short)RealtimeEventType.AccountCleanupCompleted,
            (short)RealtimeEventType.AttachmentBlobsPurge
        };
        var outboxDeleted = await dbContext.Outbox
            .Where(o => o.TargetUserId == userId && !keepTypes.Contains(o.EventType))
            .ExecuteDeleteAsync(ct)
            .ConfigureAwait(false);

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

    public async Task EnqueueEventAsync(RealtimeEvent eventToPublish, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventToPublish);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventToPublish.EventId);

        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);

        if (await dbContext.Outbox.AnyAsync(item => item.EventId == eventToPublish.EventId, ct)
                .ConfigureAwait(false))
            return;

        dbContext.Outbox.Add(CreateOutboxEntity(eventToPublish));
        await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
    }
}

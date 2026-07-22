using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class EfCoreRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly IDbContextFactory<RealtimeDbContext> _dbContextFactory;
    private readonly ILogger<EfCoreRealtimeMessageStore> _logger;

    public EfCoreRealtimeMessageStore(
        IDbContextFactory<RealtimeDbContext> dbContextFactory,
        ILogger<EfCoreRealtimeMessageStore> logger)
    {
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    public async Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);

        await using var transaction = await dbContext.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

        var existingMessageId = await dbContext.Messages
            .Where(m => m.SenderUserId == message.SenderUserId
                        && m.ClientMessageId == message.ClientMessageId)
            .Select(m => m.MessageId)
            .SingleOrDefaultAsync(ct)
            .ConfigureAwait(false);

        if (existingMessageId is not null)
        {
            await EnsureOutboxAsync(dbContext, eventToPublish, existingMessageId, ct).ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            _logger.LogInformation(
                "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return new RealtimeMessagePersistResult(false, existingMessageId);
        }

        dbContext.Messages.Add(new RealtimeMessageEntity
        {
            MessageId = message.MessageId,
            ClientMessageId = message.ClientMessageId,
            SenderUserId = message.SenderUserId,
            SenderSessionId = message.SenderSessionId,
            ReceiverUserId = message.ReceiverUserId,
            Content = message.Content,
            ReceivedAtMs = message.ReceivedAtMs
        });
        dbContext.Outbox.Add(CreateOutboxEntity(eventToPublish, message.MessageId));

        try
        {
            await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateException ex)
            when (ex.InnerException is PostgresException pgEx
                  && pgEx.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            dbContext.ChangeTracker.Clear();
            var concurrentMessageId = await dbContext.Messages
                .Where(m => m.SenderUserId == message.SenderUserId
                            && m.ClientMessageId == message.ClientMessageId)
                .Select(m => m.MessageId)
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);
            if (concurrentMessageId is null)
                throw;

            _logger.LogInformation(
                "实时消息存在（并发写入检测），跳过重复。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return new RealtimeMessagePersistResult(false, concurrentMessageId);
        }

        _logger.LogInformation(
            "实时消息已写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        return new RealtimeMessagePersistResult(true, message.MessageId);
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
    private static async Task EnsureOutboxAsync(
        RealtimeDbContext dbContext,
        RealtimeEvent evt,
        string messageId,
        CancellationToken ct)
    {
        if (await dbContext.Outbox.AnyAsync(item => item.EventId == evt.EventId, ct).ConfigureAwait(false))
            return;

        dbContext.Outbox.Add(CreateOutboxEntity(evt, messageId));
        await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
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
            EventType = (short)evt.Type
        };
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

        // Outbox：按 typed target_user_id 精确清理（含已发布与未发布），保留 AccountCleanupCompleted。
        var keepType = (short)RealtimeEventType.AccountCleanupCompleted;
        var outboxDeleted = await dbContext.Outbox
            .Where(o => o.TargetUserId == userId && o.EventType != keepType)
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

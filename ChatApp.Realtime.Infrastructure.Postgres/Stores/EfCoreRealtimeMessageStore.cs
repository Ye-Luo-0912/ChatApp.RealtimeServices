using ChatApp.Realtime.Abstractions.Stores;
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

    public async Task<bool> SaveAsync(RealtimeMessageRecord message, CancellationToken ct = default)
    {
        await using var dbContext = await _dbContextFactory
            .CreateDbContextAsync(ct)
            .ConfigureAwait(false);

        var exists = await dbContext.Messages
            .AnyAsync(m => m.SenderUserId == message.SenderUserId
                        && m.ClientMessageId == message.ClientMessageId, ct)
            .ConfigureAwait(false);

        if (exists)
        {
            _logger.LogInformation(
                "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return false;
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

        try
        {
            await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateException ex)
            when (ex.InnerException is PostgresException pgEx
                  && pgEx.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            _logger.LogInformation(
                "实时消息存在（并发写入检测），跳过重复。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
                message.ClientMessageId,
                message.SenderUserId);
            return false;
        }

        _logger.LogInformation(
            "实时消息已写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        return true;
    }
}

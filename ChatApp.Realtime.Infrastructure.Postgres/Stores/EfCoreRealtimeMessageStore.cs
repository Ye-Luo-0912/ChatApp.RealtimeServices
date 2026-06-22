using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Data.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

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

        await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);

        _logger.LogInformation(
            "实时消息已写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        return true;
    }
}

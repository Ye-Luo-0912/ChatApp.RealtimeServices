using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultUserAccountDeletedProcessor(
    IRealtimeMessageStore messageStore,
    IRealtimeOutboxSignal outboxSignal,
    ILogger<DefaultUserAccountDeletedProcessor> logger) : IUserAccountDeletedProcessor
{
    public async Task<MessageProcessResult> ProcessAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        if (evt.Type != RealtimeEventType.UserAccountDeleted)
            return MessageProcessResult.Success(evt.EventId);

        if (evt.TargetUserId <= 0)
            return MessageProcessResult.Failed("invalid_user", "TargetUserId 无效");

        try
        {
            var deleted = await messageStore
                .DeleteByUserAsync(evt.TargetUserId, batchSize: 1000, ct)
                .ConfigureAwait(false);

            logger.LogInformation(
                "账号删除清理完成。事件={EventId}；用户={UserId}；删除消息数={Deleted}",
                evt.EventId,
                evt.TargetUserId,
                deleted);

            // 完成回传走事务 Outbox，由 OutboxPublisherWorker 持久发布（非 best-effort）。
            await messageStore.EnqueueEventAsync(
                new RealtimeEvent
                {
                    EventId = $"cleanup-done:{evt.EventId}",
                    Type = RealtimeEventType.AccountCleanupCompleted,
                    TargetUserId = evt.TargetUserId,
                    ActorUserId = evt.ActorUserId,
                    OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    PayloadJson = evt.PayloadJson,
                    TraceParent = evt.TraceParent,
                    TraceState = evt.TraceState,
                },
                ct).ConfigureAwait(false);
            outboxSignal.Notify();

            return MessageProcessResult.Success(evt.EventId);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "账号删除清理失败（可重试）。事件={EventId}；用户={UserId}",
                evt.EventId,
                evt.TargetUserId);
            return MessageProcessResult.Failed("cleanup_transient", ex.Message);
        }
    }
}

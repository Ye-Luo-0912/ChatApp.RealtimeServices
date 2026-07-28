using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

/// <summary>
/// 账号删除事件处理器（轻量入口）。
/// <para>
/// LongTerm-2：原内联清理逻辑（一次性加载全部附件键 + 每批 Outbox 事务）已迁移至
/// <c>AccountCleanupWorker</c> 的可续跑 Saga。本处理器仅负责写入 tombstone（Deleting 屏障）
/// 与入队清理作业（pending, phase=attachments），立即返回成功，使 NATS 消息快速 ACK 释放队列。
/// </para>
/// <para>
/// 重量级清理由 Saga Worker 按 phase 分批推进：attachments → metadata → completed，
/// 每批 200 个对象，通过 cursor 断点续跑，失败有上限。
/// </para>
/// </summary>
public sealed class DefaultUserAccountDeletedProcessor(
    IAccountCleanupJobStore jobStore,
    IUserDeletionTombstoneStore tombstoneStore,
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
            // LongTerm-1：在账号删除清理开始前先写入 tombstone（幂等，PK=user_id）。
            // 确保清理过程中 Incoming Processor 能立即拒绝该用户的旧命令回放。
            var deletedAtMs = evt.OccurredAtMs > 0
                ? evt.OccurredAtMs
                : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            await tombstoneStore
                .RecordDeletionAsync(evt.TargetUserId, evt.EventId, deletedAtMs, ct)
                .ConfigureAwait(false);

            // LongTerm-2：入队可续跑清理作业（pending, phase=attachments）。
            // 幂等：若 (user_id, phase=attachments) 已存在则不覆盖，直接返回。
            // 重型清理由 AccountCleanupWorker Saga 按 phase 分批推进。
            await jobStore
                .EnqueueJobAsync(evt.TargetUserId, deletedAtMs, ct)
                .ConfigureAwait(false);

            logger.LogInformation(
                "账号删除清理作业已入队，等待 Saga Worker 处理。事件={EventId}；用户={UserId}",
                evt.EventId,
                evt.TargetUserId);

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
                "账号删除清理入队失败（可重试）。事件={EventId}；用户={UserId}",
                evt.EventId,
                evt.TargetUserId);
            return MessageProcessResult.Failed("cleanup_transient", ex.Message);
        }
    }
}

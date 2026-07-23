using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultUserAccountDeletedProcessor(
    IRealtimeMessageStore messageStore,
    IRealtimeAttachmentStore attachmentStore,
    IRealtimeDeviceSyncCursorStore deviceSyncCursorStore,
    IRealtimeOutboxSignal outboxSignal,
    ILogger<DefaultUserAccountDeletedProcessor> logger) : IUserAccountDeletedProcessor
{
    public const int AttachmentPurgeChunkSize = 200;

    public async Task<MessageProcessResult> ProcessAsync(RealtimeEvent evt, CancellationToken ct = default)
    {
        if (evt.Type != RealtimeEventType.UserAccountDeleted)
            return MessageProcessResult.Success(evt.EventId);

        if (evt.TargetUserId <= 0)
            return MessageProcessResult.Failed("invalid_user", "TargetUserId 无效");

        try
        {
            // 先列出 object_key 并写入 purge Outbox，再删元数据。
            // 若在「删元数据」与「写 Outbox」之间崩溃，重试时键已丢失 → Blob 永久泄漏。
            var objectKeys = await attachmentStore
                .ListObjectKeysByUserAsync(evt.TargetUserId, batchSize: 1000, ct)
                .ConfigureAwait(false);

            if (objectKeys.Count > 0)
            {
                var chunkCount = (objectKeys.Count + AttachmentPurgeChunkSize - 1)
                    / AttachmentPurgeChunkSize;
                for (var i = 0; i < chunkCount; i++)
                {
                    var chunk = objectKeys
                        .Skip(i * AttachmentPurgeChunkSize)
                        .Take(AttachmentPurgeChunkSize)
                        .ToArray();
                    await messageStore.EnqueueEventAsync(
                        new RealtimeEvent
                        {
                            EventId = RealtimeEventContracts.CreateAttachmentBlobsPurgeEventId(
                                evt.EventId,
                                i),
                            Type = RealtimeEventType.AttachmentBlobsPurge,
                            TargetUserId = evt.TargetUserId,
                            ActorUserId = evt.ActorUserId,
                            OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                            PayloadJson = JsonSerializer.Serialize(
                                new AttachmentBlobsPurgePayload
                                {
                                    UserId = evt.TargetUserId,
                                    ObjectKeys = chunk,
                                    ChunkIndex = i,
                                    ChunkCount = chunkCount
                                },
                                RealtimeJsonSerializerContext.Default.AttachmentBlobsPurgePayload),
                            TraceParent = evt.TraceParent,
                            TraceState = evt.TraceState
                        },
                        ct).ConfigureAwait(false);
                }
            }

            // purge 已落 Outbox（幂等 EventId）；再删附件元数据、设备游标与消息。
            // DeleteByUser 会清理普通 Outbox，但保留 AttachmentBlobsPurge / AccountCleanupCompleted。
            await attachmentStore
                .DeleteByUserAsync(evt.TargetUserId, batchSize: 1000, ct)
                .ConfigureAwait(false);

            var cursorDeleted = await deviceSyncCursorStore
                .DeleteByUserAsync(evt.TargetUserId, ct)
                .ConfigureAwait(false);

            var deleted = await messageStore
                .DeleteByUserAsync(evt.TargetUserId, batchSize: 1000, ct)
                .ConfigureAwait(false);

            logger.LogInformation(
                "账号删除清理完成。事件={EventId}；用户={UserId}；删除消息数={Deleted}；附件键数={KeyCount}；设备游标={Cursors}",
                evt.EventId,
                evt.TargetUserId,
                deleted,
                objectKeys.Count,
                cursorDeleted);

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

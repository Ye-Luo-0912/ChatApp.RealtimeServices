using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeAttachmentStore(ILogger<NoopRealtimeAttachmentStore> logger)
    : IRealtimeAttachmentStore
{
    public Task<RealtimeAttachmentRecord> InsertConfirmedAsync(
        RealtimeAttachmentRecord attachment,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogCritical("未配置附件存储，拒绝写入确认附件。附件={AttachmentId}", attachment.AttachmentId);
        throw new InvalidOperationException("未配置真实附件存储。");
    }

    public Task<int> BindToMessageAsync(
        string messageId,
        string? conversationId,
        long uploaderUserId,
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (attachmentIds.Count == 0)
            return Task.FromResult(0);
        logger.LogCritical("未配置附件存储，拒绝绑定附件。消息={MessageId}", messageId);
        throw new InvalidOperationException("未配置真实附件存储。");
    }

    public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>([]);
    }

    public Task<IReadOnlyList<RealtimeAttachmentRecord>> ListForUserExportAsync(
        long userId,
        string? afterAttachmentId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<RealtimeAttachmentRecord>>([]);
    }

    public Task<IReadOnlyList<string>> ListObjectKeysByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<IReadOnlyList<string>> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ct.ThrowIfCancellationRequested();
        logger.LogInformation("P0 默认实现跳过附件清理。用户={UserId}", userId);
        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<int> DeleteByAttachmentIdsAsync(
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (attachmentIds.Count == 0)
            return Task.FromResult(0);
        logger.LogCritical("未配置附件存储，拒绝按 ID 批量删除附件。数量={Count}", attachmentIds.Count);
        throw new InvalidOperationException("未配置真实附件存储。");
    }
    public Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        logger.LogCritical("未配置附件存储，拒绝确认上传。附件={AttachmentId}", attachmentId);
        throw new InvalidOperationException("未配置真实附件存储。");
    }
}

using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Attachments;

/// <summary>
/// 附件上传确认处理器：调用 <see cref="IRealtimeAttachmentStore.FinalizeUploadAsync"/>
/// 完成 Ticketed(0) → Uploaded(4) 状态转换，并映射为 <see cref="AttachmentFinalizeResult"/>。
/// </summary>
public sealed class DefaultAttachmentFinalizeProcessor : IAttachmentFinalizeProcessor
{
    private readonly IRealtimeAttachmentStore _store;
    private readonly ILogger<DefaultAttachmentFinalizeProcessor> _logger;

    public DefaultAttachmentFinalizeProcessor(
        IRealtimeAttachmentStore store,
        ILogger<DefaultAttachmentFinalizeProcessor> logger)
    {
        _store = store;
        _logger = logger;
    }

    public async Task<AttachmentFinalizeResult> ProcessAsync(
        AttachmentFinalizeCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        try
        {
            var persist = await _store
                .FinalizeUploadAsync(
                    command.ActorUserId,
                    command.AttachmentId,
                    command.SizeBytes,
                    command.ContentHash,
                    ct)
                .ConfigureAwait(false);

            if (!persist.Succeeded)
            {
                return AttachmentFinalizeResult.Failed(
                    command.RequestId,
                    persist.ErrorCode ?? "finalize_failed",
                    persist.ErrorMessage ?? "附件上传确认失败。");
            }

            var record = persist.Record!;
            return AttachmentFinalizeResult.Success(
                command.RequestId,
                record.AttachmentId,
                (short)record.Status);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "附件上传确认处理异常。请求编号={RequestId}；附件={AttachmentId}",
                command.RequestId,
                command.AttachmentId);
            return AttachmentFinalizeResult.Failed(
                command.RequestId,
                "attachment_finalize_unavailable",
                "附件上传确认服务暂时不可用。");
        }
    }

    private static AttachmentFinalizeResult? Validate(AttachmentFinalizeCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return AttachmentFinalizeResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (command.ActorUserId <= 0)
            return AttachmentFinalizeResult.Failed(
                command.RequestId,
                "invalid_user_id",
                "操作用户编号必须大于 0。");
        if (string.IsNullOrWhiteSpace(command.AttachmentId) || command.AttachmentId.Length > 128)
            return AttachmentFinalizeResult.Failed(
                command.RequestId,
                "invalid_attachment_id",
                "附件编号无效。");
        if (command.SizeBytes < 0)
            return AttachmentFinalizeResult.Failed(
                command.RequestId,
                "invalid_size",
                "附件大小不能为负数。");
        if (command.ContentHash is { Length: > 128 })
            return AttachmentFinalizeResult.Failed(
                command.RequestId,
                "invalid_content_hash",
                "内容哈希长度不能超过 128。");
        return null;
    }
}

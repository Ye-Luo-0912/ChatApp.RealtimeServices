using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Attachments;

/// <summary>
/// P1-3：附件扫描处理器。执行 Uploaded → Scanning → Available | Rejected 状态转换，
/// 全程带 state_version 条件更新，并在 Pass 时对对象存储做 HEAD 校验
/// （实际 Size / Hash / Content-Type 与票证一致，不一致则判为 Rejected）。
/// </summary>
public sealed class AttachmentScanProcessor : IAttachmentScanProcessor
{
    private readonly IRealtimeAttachmentStore _store;
    private readonly IObjectStorage? _objectStorage;
    private readonly ILogger<AttachmentScanProcessor> _logger;

    public AttachmentScanProcessor(
        IRealtimeAttachmentStore store,
        ILogger<AttachmentScanProcessor> logger,
        IObjectStorage? objectStorage = null)
    {
        _store = store;
        _logger = logger;
        _objectStorage = objectStorage;
    }

    public async Task<AttachmentScanResult> ProcessAsync(
        AttachmentScanCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
            return validationError;

        try
        {
            // 1) 扫描开抢：Uploaded → Scanning（条件更新，探测旧版本）。
            var begin = await _store
                .BeginScanAsync(command.AttachmentId, command.StateVersion, ct)
                .ConfigureAwait(false);
            if (!begin.Succeeded)
            {
                return AttachmentScanResult.Failed(
                    command.RequestId,
                    begin.ErrorCode ?? "scan_begin_failed",
                    begin.ErrorMessage ?? "无法开始扫描（状态或版本不匹配）。");
            }

            var record = begin.Record!;

            // 2) 对象存储 HEAD 校验（仅 Pass 判定时核对元数据一致性）。
            var verdict = command.Verdict;
            var head = _objectStorage is not null
                ? await _objectStorage.HeadAsync(record.ObjectKey, ct).ConfigureAwait(false)
                : null;
            string? rejectReason = null;

            if (verdict == AttachmentScanVerdict.Pass && head is not null)
            {
                if (head.Value.SizeBytes != record.SizeBytes)
                {
                    verdict = AttachmentScanVerdict.Reject;
                    rejectReason = $"对象实际大小 {head.Value.SizeBytes} 与票证 {record.SizeBytes} 不一致。";
                }
                else if (head.Value.ContentType is not null
                         && !string.Equals(
                             head.Value.ContentType,
                             record.ContentType,
                             StringComparison.OrdinalIgnoreCase))
                {
                    verdict = AttachmentScanVerdict.Reject;
                    rejectReason = $"对象 Content-Type {head.Value.ContentType} 与票证 {record.ContentType} 不一致。";
                }
            }

            // 3) 扫描完成：Scanning → Available | Rejected（条件更新，防旧结果覆盖）。
            var complete = await _store
                .CompleteScanAsync(
                    record.AttachmentId,
                    record.StateVersion,
                    verdict,
                    head?.SizeBytes ?? record.SizeBytes,
                    head?.ContentHash ?? command.ContentHash,
                    head?.ContentType ?? record.ContentType,
                    rejectReason ?? command.Reason,
                    ct)
                .ConfigureAwait(false);

            if (!complete.Succeeded)
            {
                return AttachmentScanResult.Failed(
                    command.RequestId,
                    complete.ErrorCode ?? "scan_complete_failed",
                    complete.ErrorMessage ?? "无法完成扫描状态转换。");
            }

            return AttachmentScanResult.Success(
                command.RequestId,
                complete.Record!.AttachmentId,
                (short)complete.Record.Status);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "附件扫描处理异常。请求编号={RequestId}；附件={AttachmentId}",
                command.RequestId,
                command.AttachmentId);
            return AttachmentScanResult.Failed(
                command.RequestId,
                "attachment_scan_unavailable",
                "附件扫描服务暂时不可用。");
        }
    }

    private static AttachmentScanResult? Validate(AttachmentScanCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.RequestId) || command.RequestId.Length > 64)
            return AttachmentScanResult.Failed(
                command.RequestId ?? string.Empty,
                "invalid_request_id",
                "请求编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.AttachmentId) || command.AttachmentId.Length > 128)
            return AttachmentScanResult.Failed(
                command.RequestId,
                "invalid_attachment_id",
                "附件编号无效。");
        if (command.SizeBytes < 0)
            return AttachmentScanResult.Failed(
                command.RequestId,
                "invalid_size",
                "附件大小不能为负数。");
        if (command.ContentHash is { Length: > 128 })
            return AttachmentScanResult.Failed(
                command.RequestId,
                "invalid_content_hash",
                "内容哈希长度不能超过 128。");
        return null;
    }
}
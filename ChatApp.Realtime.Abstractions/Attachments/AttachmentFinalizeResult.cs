namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 附件上传确认结果。
/// </summary>
public sealed class AttachmentFinalizeResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public string? AttachmentId { get; init; }
    /// <summary>确认后的状态（AttachmentStatus 数值：Uploaded=4 / Rejected=6 等）。</summary>
    public short? Status { get; init; }

    public static AttachmentFinalizeResult Success(
        string requestId,
        string attachmentId,
        short status) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            AttachmentId = attachmentId,
            Status = status
        };

    public static AttachmentFinalizeResult Failed(
        string requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };

    public static AttachmentFinalizeResult ServerBusy(
        string requestId,
        int retryAfterMs,
        string queueKind) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = "server_busy",
            ErrorMessage = "服务繁忙，请稍后重试。",
            RetryAfterMs = retryAfterMs,
            QueueKind = queueKind
        };
}

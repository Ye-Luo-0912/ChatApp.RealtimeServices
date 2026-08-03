namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>附件扫描结果（映射回扫描服务确认）。</summary>
public sealed class AttachmentScanResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? AttachmentId { get; init; }
    /// <summary>转换后的状态（AttachmentStatus 数值：Available=7 / Rejected=6）。</summary>
    public short? Status { get; init; }

    public static AttachmentScanResult Success(string requestId, string attachmentId, short status) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            AttachmentId = attachmentId,
            Status = status
        };

    public static AttachmentScanResult Failed(string requestId, string errorCode, string errorMessage) =>
        new() { RequestId = requestId, Succeeded = false, ErrorCode = errorCode, ErrorMessage = errorMessage };
}
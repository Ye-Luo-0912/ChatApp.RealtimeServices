namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 附件下载授权结果。
/// </summary>
public sealed class AttachmentDownloadAuthorizeResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public string? AttachmentId { get; init; }
    /// <summary>签发的短时有效下载 URL（成功时）。</summary>
    public string? DownloadUrl { get; init; }
    /// <summary>签名令牌（若 URL 需携带令牌鉴权）。</summary>
    public string? DownloadToken { get; init; }
    /// <summary>下载 URL 过期时间（unix 毫秒）。</summary>
    public long? ExpiresAtMs { get; init; }

    public static AttachmentDownloadAuthorizeResult Success(
        string requestId,
        string attachmentId,
        string downloadUrl,
        string? downloadToken,
        long? expiresAtMs) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            AttachmentId = attachmentId,
            DownloadUrl = downloadUrl,
            DownloadToken = downloadToken,
            ExpiresAtMs = expiresAtMs
        };

    public static AttachmentDownloadAuthorizeResult Failed(
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
}
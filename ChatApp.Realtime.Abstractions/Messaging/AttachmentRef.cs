namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 消息附件线协议引用（版本化）。下载经 Server API，不含永久公网 URL。
/// </summary>
public sealed class AttachmentRef
{
    public const int CurrentVersion = 1;

    /// <summary>引用结构版本；旧客户端可忽略未知字段。</summary>
    public int RefVersion { get; init; } = CurrentVersion;

    public required string AttachmentId { get; init; }

    public string? FileName { get; init; }

    public required string ContentType { get; init; }

    public long SizeBytes { get; init; }

    /// <summary>客户端可见状态：扫描中 / 可下载。</summary>
    public AttachmentWireStatus Status { get; init; }

    /// <summary>
    /// 下载提示：通常为 attachmentId，客户端请求
    /// <c>GET /api/attachments/{id}/download</c>（非永久公网 URL）。
    /// </summary>
    public string? DownloadApiHint { get; init; }

    /// <summary>可选短时下载令牌（由 Server 签发时填充）。</summary>
    public string? DownloadToken { get; init; }

    /// <summary>可选缩略图提示（同 DownloadApiHint 语义，路径由 Server 约定）。</summary>
    public string? ThumbnailApiHint { get; init; }
}

/// <summary>附件对客户端的可用性（与库内 Ticketed/Confirmed/Bound 生命周期解耦）。</summary>
public enum AttachmentWireStatus : short
{
    Scanning = 0,
    Available = 1
}

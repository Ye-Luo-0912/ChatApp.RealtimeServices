namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>扫描结果判定。</summary>
public enum AttachmentScanVerdict : byte
{
    /// <summary>通过（元数据一致 + 无恶意/违规内容）。</summary>
    Pass = 0,

    /// <summary>拒绝（恶意/违规/元数据不符）。</summary>
    Reject = 1
}

/// <summary>
/// 附件扫描结果（扫描服务回调）。驱动 Uploaded → Scanning → Available | Rejected 状态转换。
/// 必须携带 <see cref="StateVersion"/>（扫描开抢时的版本），落库时用条件更新防止旧结果覆盖新状态。
/// </summary>
public sealed class AttachmentScanCommand
{
    public required string RequestId { get; init; }
    public required string AttachmentId { get; init; }
    public required AttachmentScanVerdict Verdict { get; init; }

    /// <summary>扫描开抢时的状态版本号；落库条件更新时必须匹配。</summary>
    public long StateVersion { get; init; }

    /// <summary>扫描得到的实际元数据（与票证核对；仅 Pass 时校验）。</summary>
    public long SizeBytes { get; init; }
    public string? ContentHash { get; init; }
    public string? ContentType { get; init; }

    /// <summary>拒绝原因（Verdict=Reject 时可选）。</summary>
    public string? Reason { get; init; }
}
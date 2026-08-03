namespace ChatApp.Realtime.Abstractions.Stores;

public sealed class RealtimeAttachmentRecord
{
    public required string AttachmentId { get; init; }
    public required long UploaderUserId { get; init; }
    public required string ObjectKey { get; init; }
    public string? PublicUrl { get; init; }
    public required string ContentType { get; init; }
    public required long SizeBytes { get; init; }
    public string? OriginalName { get; init; }
    public required AttachmentStatus Status { get; init; }
    public string? MessageId { get; init; }
    public string? ConversationId { get; init; }
    public string? ClientAttachmentId { get; init; }
    public long CreatedAtMs { get; init; }
    public long? ConfirmedAtMs { get; init; }
    public long? BoundAtMs { get; init; }

    /// <summary>SHA-256 十六进制（小写），上传或扫描写入；可空。</summary>
    public string? ContentHash { get; init; }

    /// <summary>
    /// 状态版本号。每次状态转换必须递增并使用条件更新（<c>WHERE state_version = @旧值</c>），
    /// 防止旧扫描结果/旧回调覆盖新状态（ABA 防护）。
    /// </summary>
    public long StateVersion { get; init; }
}
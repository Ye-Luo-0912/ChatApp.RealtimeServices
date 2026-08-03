namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 附件上传确认命令（Core NATS request/reply）。
/// <para>
/// 主线四：客户端完成分片上传后，Gateway 经 <c>IRealtimeMessageBus.FinalizeAttachmentUploadAsync</c>
/// 转发到 Server，触发 Realtime 侧 Ticketed(0) → Uploaded(4) 状态转换。
/// </para>
/// </summary>
public sealed class AttachmentFinalizeCommand
{
    public required string RequestId { get; init; }
    public long ActorUserId { get; init; }
    public required string AttachmentId { get; init; }
    public long SizeBytes { get; init; }
    /// <summary>SHA-256 十六进制（小写）；可空。</summary>
    public string? ContentHash { get; init; }
    /// <summary>上行会话 Id（幂等 / 回声跳过）。</summary>
    public string? ActorSessionId { get; init; }
}

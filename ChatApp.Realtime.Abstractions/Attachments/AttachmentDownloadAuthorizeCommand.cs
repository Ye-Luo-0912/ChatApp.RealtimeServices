namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 附件下载授权命令（Core NATS request/reply）。
/// <para>
/// P1-3：客户端请求为附件签发短时有效的签名下载 URL。Gateway 经
/// <c>IRealtimeMessageBus.AuthorizeAttachmentDownloadAsync</c> 转发到 Server，
/// 由 Server 调用对象存储签发 URL 后返回 <see cref="AttachmentDownloadAuthorizeResult"/>。
/// </para>
/// </summary>
public sealed class AttachmentDownloadAuthorizeCommand
{
    public required string RequestId { get; init; }
    public long ActorUserId { get; init; }
    public required string AttachmentId { get; init; }
    /// <summary>可选：附件所属会话 Id，辅助权限校验。</summary>
    public string? ConversationId { get; init; }
    /// <summary>上行会话 Id（幂等 / 回声跳过）。</summary>
    public string? ActorSessionId { get; init; }
}
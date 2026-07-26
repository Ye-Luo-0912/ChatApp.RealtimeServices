namespace ChatApp.Realtime.Abstractions.Events;

public sealed class RealtimeAttachmentLifecyclePayload
{
    public const int CurrentPayloadVersion = 1;

    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    public required string AttachmentId { get; init; }

    /// <summary>
    /// 客户端面向状态：2=UploadConfirmed, 4=Scanning, 1=Available, 6=Rejected(映射服务端), 5=Expired, 7=ThumbnailUpdated。
    /// 取值与 ChatApp.TcpGateway.Core.Messaging.AttachmentWireStatus 对齐。
    /// </summary>
    public short Status { get; init; }

    public long OccurredAtMs { get; init; }

    public string? RejectReason { get; init; }

    public string? ThumbnailApiHint { get; init; }

    public string? DownloadToken { get; init; }
}

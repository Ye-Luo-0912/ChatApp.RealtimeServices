namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 消息发送前附件绑定校验的稳定错误码（VOICE-MSG-1）。
/// <para>
/// 发送前必须再次确认附件为 Available（或 legacy Confirmed）且归属/绑定合法；
/// 扫描中、拒绝、过期、已绑定冲突、不存在或非本人附件返回各自稳定的错误码，绝不静默跳过。
/// </para>
/// </summary>
public enum AttachmentBindErrorCode : byte
{
    /// <summary>无错误（绑定成功）。</summary>
    None = 0,

    /// <summary>附件不存在。</summary>
    NotFound = 1,

    /// <summary>附件不属于当前发送方（归属非法）。</summary>
    Forbidden = 2,

    /// <summary>附件仍在扫描中，未就绪。</summary>
    Scanning = 3,

    /// <summary>附件已被扫描拒绝。</summary>
    Rejected = 4,

    /// <summary>附件已过期。</summary>
    Expired = 5,

    /// <summary>附件已绑定到其它消息（已绑定冲突）。</summary>
    AlreadyBound = 6,

    /// <summary>附件处于不可绑定的其它状态（如 Ticketed/Uploaded/Abandoned）。</summary>
    InvalidState = 7
}

public static class AttachmentBindErrorCodeExtensions
{
    /// <summary>稳定的底层错误码字符串，用于日志与诊断，不依赖本地化。</summary>
    public static string ToStableCode(this AttachmentBindErrorCode code) => code switch
    {
        AttachmentBindErrorCode.None => "none",
        AttachmentBindErrorCode.NotFound => "attachment_not_found",
        AttachmentBindErrorCode.Forbidden => "attachment_forbidden",
        AttachmentBindErrorCode.Scanning => "attachment_scanning",
        AttachmentBindErrorCode.Rejected => "attachment_rejected",
        AttachmentBindErrorCode.Expired => "attachment_expired",
        AttachmentBindErrorCode.AlreadyBound => "attachment_already_bound",
        AttachmentBindErrorCode.InvalidState => "attachment_invalid_state",
        _ => "attachment_bind_failed"
    };
}
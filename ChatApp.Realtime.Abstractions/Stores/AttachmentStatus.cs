namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 附件生命周期：票证 → 已上传 → 扫描 → 已确认 → 已绑定消息（或废弃/拒绝）。
/// 数值与 Server Core.Models.Export.AttachmentStatus 保持一致。
/// </summary>
public enum AttachmentStatus : short
{
    Ticketed = 0,
    Confirmed = 1,
    Bound = 2,
    Abandoned = 3,
    Uploaded = 4,
    Scanning = 5,
    Rejected = 6
}

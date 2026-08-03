namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 附件生命周期：票证 → 已上传 → 扫描 → 可用/拒绝 → 已绑定消息（或废弃/过期）。
/// 数值与 Server Core.Models.Export.AttachmentStatus 保持一致。
/// </summary>
public enum AttachmentStatus : short
{
    /// <summary>已签发上传票证，尚未上传完成。</summary>
    Ticketed = 0,

    /// <summary>已确认上传（Server 直连确认路径）。</summary>
    Confirmed = 1,

    /// <summary>已绑定到消息。</summary>
    Bound = 2,

    /// <summary>已废弃（未绑定且无法恢复）。</summary>
    Abandoned = 3,

    /// <summary>已上传完成，等待扫描。</summary>
    Uploaded = 4,

    /// <summary>扫描中。</summary>
    Scanning = 5,

    /// <summary>扫描拒绝（恶意/违规/元数据不符）。</summary>
    Rejected = 6,

    /// <summary>扫描通过，可下载（尚未绑定消息）。</summary>
    Available = 7,

    /// <summary>已过期（未绑定超过保留期，对象即将删除）。</summary>
    Expired = 8
}
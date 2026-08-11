namespace ChatApp.RealtimeServices.Options;

/// <summary>
/// 未绑定附件过期清理（sweep）配置。驱动 <c>AttachmentSweepWorker</c>
/// 周期性调用 <c>IAttachmentSweeper.SweepAsync</c>，把超过保留期、未绑定消息的
/// Ticketed/Uploaded/Scanning 附件标记为 Expired。
/// </summary>
public sealed class AttachmentSweepOptions
{
    public const string SectionName = "AttachmentSweep";

    /// <summary>总开关。false 时 worker 保持空闲。</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>两次清理周期之间的轮询间隔（毫秒）。</summary>
    public int IntervalMs { get; init; } = 60_000;

    /// <summary>未绑定附件的保留期（天）。</summary>
    public int RetentionDays { get; init; } = 7;
}
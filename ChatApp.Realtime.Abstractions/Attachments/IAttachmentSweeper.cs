namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 未绑定附件过期清理器：把长期处于 Ticketed/Uploaded/Scanning 且未绑定消息的附件
/// 标记为 Expired（对象随后由对象存储清理），带 state_version 条件更新。
/// </summary>
public interface IAttachmentSweeper
{
    /// <summary>
    /// 执行一轮清理：扫描过期候选并标记。返回本轮回合处理的候选数。
    /// </summary>
    Task<int> SweepAsync(CancellationToken ct = default);
}
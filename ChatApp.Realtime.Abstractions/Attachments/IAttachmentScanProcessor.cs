namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 附件扫描处理器：HEAD 校验对象元数据与票证一致，然后驱动
/// Uploaded → Scanning → Available | Rejected 状态转换（全部带 state_version 条件更新）。
/// </summary>
public interface IAttachmentScanProcessor
{
    /// <summary>扫描回调：校验并完成一次扫描。</summary>
    Task<AttachmentScanResult> ProcessAsync(
        AttachmentScanCommand command,
        CancellationToken ct = default);
}

/// <summary>扫描消费者（从 NATS 订阅扫描结果）。</summary>
public interface IAttachmentScanConsumer
{
    IAsyncEnumerable<AttachmentScanCommand> ConsumeAsync(CancellationToken ct = default);
}
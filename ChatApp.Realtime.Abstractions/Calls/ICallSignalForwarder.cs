namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 即时信令（SDP/ICE）转发器。只允许经受预算限制的临时信令路径转发，
/// 不进入 PostgreSQL、持久化 Outbox、JetStream 历史或 Realtime Event 管线。
/// </summary>
public interface ICallSignalForwarder
{
    /// <summary>
    /// 将 <see cref="CallSignalEnvelope"/> 转发给目标用户当前在线的 Gateway。
    /// 必须执行预算校验（载荷大小、条数上限），任一超限即拒绝。
    /// </summary>
    /// <returns>转发是否成功；false 表示预算超限或目标不可达（fail-closed）。</returns>
    Task<bool> ForwardAsync(CallSignalEnvelope signal, CancellationToken ct = default);
}

/// <summary>
/// 通话审计存储。只保存参与者、状态、时间、失败分类与 QoE 汇总。
/// </summary>
public interface ICallAuditStore
{
    Task RecordAsync(CallAuditEntry entry, CancellationToken ct = default);

    /// <summary>按 call id 倒序读取审计（测试/诊断用）。</summary>
    Task<IReadOnlyList<CallAuditEntry>> ListAsync(string callId, CancellationToken ct = default);
}
namespace ChatApp.RealtimeServices.Options;

public sealed class RealtimeOptions
{
    public required string ServiceName { get; init; }
    public required string InstanceId { get; init; }
    public int WorkerIntervalMs { get; init; } = 1000;
    public bool EnableDetailedErrors { get; init; }
    public int ProcessingConcurrency { get; init; } = 4;
    public int ProcessingQueueCapacity { get; init; } = 512;

    /// <summary>
    /// 所有查询类 Worker（历史 / 会话列表 / 已读 / 偏好 / 同步）共享的数据库并发预算。
    /// </summary>
    public int HistoryQueryConcurrency { get; init; } = 8;

    /// <summary>每个查询 Worker 的入队容量（非总和）。</summary>
    public int HistoryQueryQueueCapacity { get; init; } = 256;

    /// <summary>
    /// 每个查询 Worker 的通道读取槽位数。实际 DB 并发仍受 <see cref="HistoryQueryConcurrency"/> 限制。
    /// </summary>
    public int HistoryQueryWorkerSlots { get; init; } = 2;
    public int TransientRetryDelayMs { get; init; } = 1000;
    public int PoisonDeliveryThreshold { get; init; } = 8;
    public int ReadinessHeartbeatTimeoutMs { get; init; } = 30_000;
}

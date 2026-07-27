namespace ChatApp.RealtimeServices.Options;

public sealed class OutboxOptions
{
    public int BatchSize { get; init; } = 100;
    public int PublishConcurrency { get; init; } = 8;
    public int PollIntervalMs { get; init; } = 200;
    public int LeaseSeconds { get; init; } = 30;
    public int MaxRetryDelaySeconds { get; init; } = 300;
    /// <summary>达到该尝试次数后进入 Dead，不再自动重试。</summary>
    public int MaxAttempts { get; init; } = 10;
    /// <summary>已发布 Outbox 行保留时长；超时由清理任务批量删除。</summary>
    public int PublishedRetentionHours { get; init; } = 168;
    public int CleanupBatchSize { get; init; } = 500;
    public int CleanupIntervalMs { get; init; } = 60_000;

    /// <summary>
    /// Perf-8：单个清理周期内允许的 Published 最大批次数。0 表示不限制（兼容旧行为）。
    /// 大积压时持续 DELETE 会产生大量 WAL 与 Vacuum 压力，限制批次数可让负载自然节流。
    /// </summary>
    public int PublishedMaxBatchesPerCycle { get; init; } = 50;

    /// <summary>
    /// Perf-8：Published 批次之间的休眠毫秒数。0 表示不休眠。
    /// 与 <see cref="PublishedMaxBatchesPerCycle"/> 配合，给下游复制/Vacuum 留出喘息窗口。
    /// </summary>
    public int PublishedBatchSleepMs { get; init; } = 100;

    /// <summary>
    /// Perf-8：Dead 行保留天数。0 表示不按 TTL 清理（仍受 <see cref="DeadMaxRows"/> 上限约束）。
    /// 建议配合 <see cref="DeadArchiveSink"/> 使用：归档后再删除。
    /// </summary>
    public int DeadRetentionDays { get; init; } = 30;

    /// <summary>
    /// Perf-8：单个清理周期内最多删除的 Dead 行数。0 表示不限制。
    /// 用于防止 Dead 行突然膨胀拖垮清理周期。
    /// </summary>
    public int DeadMaxRows { get; init; } = 5_000;

    /// <summary>
    /// Perf-8：Dead 行归档接收器名称。null 表示不归档，直接物理删除。
    /// 配置后由对应的 <c>IDeadLetterArchiveSink</c> 实现负责落盘到对象存储/审计库。
    /// </summary>
    public string? DeadArchiveSink { get; init; }
}

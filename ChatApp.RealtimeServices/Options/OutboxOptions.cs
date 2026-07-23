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
}

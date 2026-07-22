namespace ChatApp.RealtimeServices.Options;

public sealed class RealtimeOptions
{
    public required string ServiceName { get; init; }
    public required string InstanceId { get; init; }
    public int WorkerIntervalMs { get; init; } = 1000;
    public bool EnableDetailedErrors { get; init; }
    public int ProcessingConcurrency { get; init; } = 4;
    public int ProcessingQueueCapacity { get; init; } = 512;
    public int HistoryQueryConcurrency { get; init; } = 8;
    public int HistoryQueryQueueCapacity { get; init; } = 256;
    public int TransientRetryDelayMs { get; init; } = 1000;
    public int PoisonDeliveryThreshold { get; init; } = 8;
    public int ReadinessHeartbeatTimeoutMs { get; init; } = 30_000;
}

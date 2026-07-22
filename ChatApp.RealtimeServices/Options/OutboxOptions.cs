namespace ChatApp.RealtimeServices.Options;

public sealed class OutboxOptions
{
    public int BatchSize { get; init; } = 100;
    public int PublishConcurrency { get; init; } = 8;
    public int PollIntervalMs { get; init; } = 200;
    public int LeaseSeconds { get; init; } = 30;
    public int MaxRetryDelaySeconds { get; init; } = 300;
}

namespace ChatApp.RealtimeServices.Options;

public sealed class RealtimeOptions
{
    public required string ServiceName { get; init; }
    public required string InstanceId { get; init; }
    public int WorkerIntervalMs { get; init; } = 1000;
    public bool EnableDetailedErrors { get; init; }
}

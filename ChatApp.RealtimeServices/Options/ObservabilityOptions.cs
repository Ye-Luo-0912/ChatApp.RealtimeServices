namespace ChatApp.RealtimeServices.Options;

public sealed class ObservabilityOptions
{
    public const string SectionName = "Observability";

    public bool PrometheusEnabled { get; init; } = true;
    public int PrometheusCacheMilliseconds { get; init; } = 300;
    public int OutboxStatsCollectionIntervalMs { get; init; } = 5_000;
    public bool OtlpEnabled { get; init; }
    public string OtlpEndpoint { get; init; } = "http://127.0.0.1:4317";
    public double TraceSampleRatio { get; init; } = 0.05;

    public bool IsValid() =>
        PrometheusCacheMilliseconds is >= 0 and <= 60_000
        && OutboxStatsCollectionIntervalMs is >= 1_000 and <= 60_000
        && TraceSampleRatio is >= 0 and <= 1
        && (!OtlpEnabled
            || Uri.TryCreate(OtlpEndpoint, UriKind.Absolute, out _));
}

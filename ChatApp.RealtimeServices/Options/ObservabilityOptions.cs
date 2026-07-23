namespace ChatApp.RealtimeServices.Options;

public sealed class ObservabilityOptions
{
    public const string SectionName = "Observability";

    public bool PrometheusEnabled { get; init; } = true;
    public int PrometheusCacheMilliseconds { get; init; } = 300;

    /// <summary>
    /// Outbox 全量 Pending/Dead 对账间隔。热路径计数由发布成功/失败/死信直接更新；
    /// 此周期仅做低频校准（oldest age / drift）。默认 5 分钟。
    /// </summary>
    public int OutboxStatsCollectionIntervalMs { get; init; } = 300_000;

    public bool OtlpEnabled { get; init; }
    public string OtlpEndpoint { get; init; } = "http://127.0.0.1:4317";
    public double TraceSampleRatio { get; init; } = 0.05;

    public bool IsValid() =>
        PrometheusCacheMilliseconds is >= 0 and <= 60_000
        && OutboxStatsCollectionIntervalMs is >= 10_000 and <= 3_600_000
        && TraceSampleRatio is >= 0 and <= 1
        && (!OtlpEnabled
            || Uri.TryCreate(OtlpEndpoint, UriKind.Absolute, out _));
}

using System.Diagnostics.Metrics;

namespace ChatApp.Realtime.Abstractions.Diagnostics;

/// <summary>
/// 第三阶段路由分片可观测性指标。
/// <para>
/// 记录 Realtime Event / Ephemeral Typing / Ephemeral Presence 三类事件
/// 在分片模式下的命中率、广播回退率、路由目录查询延迟、批量 fanout 倍数等。
/// 用于监控分片路由的实际效果与回退比例。
/// </para>
/// </summary>
public sealed class RoutingMetrics : IDisposable
{
    public const string DefaultMeterName = "ChatApp.Realtime.Routing";

    private readonly Meter _meter;
    private readonly Counter<long> _shardPublishes;
    private readonly Counter<long> _broadcastFallback;
    private readonly Histogram<double> _directoryQueryDuration;
    private readonly Histogram<long> _directoryInstanceCount;
    private readonly Histogram<long> _fanoutTargetCount;
    private readonly Histogram<long> _fanoutInstanceCount;

    public RoutingMetrics(string meterName = DefaultMeterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meterName);
        _meter = new Meter(meterName, "1.0.0");
        _shardPublishes = _meter.CreateCounter<long>(
            "chatapp.routing.shard.publishes");
        _broadcastFallback = _meter.CreateCounter<long>(
            "chatapp.routing.broadcast.fallback");
        _directoryQueryDuration = _meter.CreateHistogram<double>(
            "chatapp.routing.directory.query.duration",
            "ms");
        _directoryInstanceCount = _meter.CreateHistogram<long>(
            "chatapp.routing.directory.query.instances");
        _fanoutTargetCount = _meter.CreateHistogram<long>(
            "chatapp.routing.fanout.targets");
        _fanoutInstanceCount = _meter.CreateHistogram<long>(
            "chatapp.routing.fanout.instances");
    }

    /// <summary>
    /// 记录一次按 Gateway 实例分片投递。
    /// </summary>
    /// <param name="channel">事件通道：realtime / typing / presence。</param>
    /// <param name="target">目标形态：single（单目标）/ many（聚合多目标）。</param>
    /// <param name="instanceCount">本次分片命中的 Gateway 实例数。</param>
    public void RecordShardPublish(string channel, string target, int instanceCount)
    {
        if (instanceCount <= 0)
            return;
        _shardPublishes.Add(
            instanceCount,
            new KeyValuePair<string, object?>("channel", Normalize(channel)),
            new KeyValuePair<string, object?>("target", Normalize(target)));
    }

    /// <summary>
    /// 记录一次回退到广播的投递。
    /// </summary>
    /// <param name="channel">事件通道：realtime / typing / presence。</param>
    /// <param name="reason">回退原因：no_pattern / empty_directory / account_cleanup / invalid_target。</param>
    public void RecordBroadcastFallback(string channel, string reason)
    {
        _broadcastFallback.Add(
            1,
            new KeyValuePair<string, object?>("channel", Normalize(channel)),
            new KeyValuePair<string, object?>("reason", Normalize(reason)));
    }

    /// <summary>
    /// 记录一次路由目录查询。
    /// </summary>
    /// <param name="kind">目录类型：gateway（用户在线 Gateway）/ watcher（被观察用户 watcher 所在 Gateway）。</param>
    /// <param name="batch">查询批量：single（单用户）/ many（批量多用户）。</param>
    /// <param name="duration">查询耗时。</param>
    /// <param name="instanceCount">返回的 Gateway 实例数（批量查询时为去重后的总实例数）。</param>
    public void RecordDirectoryQuery(
        string kind,
        string batch,
        TimeSpan duration,
        int instanceCount)
    {
        _directoryQueryDuration.Record(
            duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("kind", Normalize(kind)),
            new KeyValuePair<string, object?>("batch", Normalize(batch)));
        _directoryInstanceCount.Record(
            Math.Max(0, instanceCount),
            new KeyValuePair<string, object?>("kind", Normalize(kind)),
            new KeyValuePair<string, object?>("batch", Normalize(batch)));
    }

    /// <summary>
    /// 记录一次多目标聚合事件的 fanout 倍数。
    /// </summary>
    /// <param name="targetCount">聚合事件的 TargetUserIds 长度。</param>
    /// <param name="instanceCount">实际分片命中的 Gateway 实例数。</param>
    public void RecordFanout(int targetCount, int instanceCount)
    {
        _fanoutTargetCount.Record(Math.Max(0, targetCount));
        _fanoutInstanceCount.Record(Math.Max(0, instanceCount));
    }

    public void Dispose() => _meter.Dispose();

    private static string Normalize(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "unknown" : value;
}

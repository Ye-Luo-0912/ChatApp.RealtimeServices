using ChatApp.Realtime.Infrastructure.Nats.Configuration;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 计算 JetStream ACK 租约的实际时序。
/// </summary>
internal static class JetStreamAckTiming
{
    /// <summary>
    /// 返回 consumer 实际使用的 ACK 超时。JetStream 配置 BackOff 后会忽略 AckWait，
    /// 并将首个 BackOff 值用作第一次重投前的等待时间。
    /// </summary>
    public static TimeSpan GetEffectiveAckWait(JetStreamOptions? options)
    {
        if (options is null)
            return TimeSpan.Zero;

        var consumer = options.Consumer;
        var configuredSeconds = consumer.BackoffSeconds is { Length: > 0 }
            ? consumer.BackoffSeconds[0]
            : consumer.AckWaitSeconds;
        return TimeSpan.FromSeconds(Math.Max(1, configuredSeconds));
    }

    /// <summary>
    /// 在实际 ACK 超时的一半处续租，给调度抖动与网络往返留出余量。
    /// </summary>
    public static TimeSpan GetProgressAckInterval(TimeSpan effectiveAckWait)
    {
        if (effectiveAckWait <= TimeSpan.Zero)
            return TimeSpan.Zero;

        return TimeSpan.FromTicks(Math.Max(1, effectiveAckWait.Ticks / 2));
    }
}

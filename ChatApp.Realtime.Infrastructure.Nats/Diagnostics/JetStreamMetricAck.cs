using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Diagnostics;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Infrastructure.Nats.Diagnostics;

internal readonly record struct JetStreamDeliveryObservation(
    string Consumer,
    long StartedTimestamp);

internal static class JetStreamMetricAck
{
    public static JetStreamDeliveryObservation Observe(
        NatsTransportMetrics metrics,
        NatsJSMsgMetadata? metadata,
        string fallbackConsumer)
    {
        var consumer = metadata?.Consumer ?? fallbackConsumer;
        metrics.RecordJetStreamDelivery(
            consumer,
            metadata?.NumDelivered ?? 1,
            metadata?.NumPending ?? 0);
        return new JetStreamDeliveryObservation(
            consumer,
            Stopwatch.GetTimestamp());
    }

    public static async ValueTask AckAsync<T>(
        INatsJSMsg<T> message,
        NatsTransportMetrics metrics,
        JetStreamDeliveryObservation observation,
        CancellationToken ct)
    {
        var succeeded = false;
        try
        {
            await message.AckAsync(cancellationToken: ct).ConfigureAwait(false);
            succeeded = true;
        }
        finally
        {
            metrics.RecordJetStreamAcknowledgement(
                observation.Consumer,
                "ack",
                Stopwatch.GetElapsedTime(observation.StartedTimestamp),
                succeeded);
        }
    }

    public static async ValueTask NakAsync<T>(
        INatsJSMsg<T> message,
        NatsTransportMetrics metrics,
        JetStreamDeliveryObservation observation,
        TimeSpan? delay,
        CancellationToken ct)
    {
        var succeeded = false;
        try
        {
            await message.NakAsync(
                delay is null ? null : new AckOpts { NakDelay = delay },
                ct).ConfigureAwait(false);
            succeeded = true;
        }
        finally
        {
            metrics.RecordJetStreamAcknowledgement(
                observation.Consumer,
                "nak",
                Stopwatch.GetElapsedTime(observation.StartedTimestamp),
                succeeded);
        }
    }

    public static async ValueTask TerminateAsync<T>(
        INatsJSMsg<T> message,
        NatsTransportMetrics metrics,
        JetStreamDeliveryObservation observation,
        string reason,
        CancellationToken ct)
    {
        var succeeded = false;
        try
        {
            await message.AckTerminateAsync(
                new AckOpts { TerminateReason = reason },
                ct).ConfigureAwait(false);
            succeeded = true;
        }
        finally
        {
            metrics.RecordJetStreamAcknowledgement(
                observation.Consumer,
                "terminate",
                Stopwatch.GetElapsedTime(observation.StartedTimestamp),
                succeeded);
        }
    }

    /// <summary>
    /// Reliability-4：In-Progress ACK（JetStream WPI）。
    /// 重置 AckWait 计时器但不消费消息，防止长时处理消息在完成前被 JetStream 重投。
    /// </summary>
    public static async ValueTask ProgressAckAsync<T>(
        INatsJSMsg<T> message,
        NatsTransportMetrics metrics,
        JetStreamDeliveryObservation observation,
        CancellationToken ct)
    {
        var succeeded = false;
        try
        {
            await message.AckProgressAsync(cancellationToken: ct).ConfigureAwait(false);
            succeeded = true;
        }
        finally
        {
            metrics.RecordJetStreamAcknowledgement(
                observation.Consumer,
                "progress",
                Stopwatch.GetElapsedTime(observation.StartedTimestamp),
                succeeded);
        }
    }
}

using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Diagnostics;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Integration;

internal readonly record struct IntegrationDeliveryObservation(
    string Consumer,
    long StartedTimestamp);

internal static class IntegrationJetStreamMetricAck
{
    public static IntegrationDeliveryObservation Observe(
        NatsTransportMetrics metrics,
        NatsJSMsgMetadata? metadata,
        string fallbackConsumer)
    {
        var consumer = metadata?.Consumer ?? fallbackConsumer;
        metrics.RecordJetStreamDelivery(
            consumer,
            metadata?.NumDelivered ?? 1,
            metadata?.NumPending ?? 0);
        return new IntegrationDeliveryObservation(
            consumer,
            Stopwatch.GetTimestamp());
    }

    public static async ValueTask AckAsync<T>(
        INatsJSMsg<T> message,
        NatsTransportMetrics metrics,
        IntegrationDeliveryObservation observation,
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
        IntegrationDeliveryObservation observation,
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
        IntegrationDeliveryObservation observation,
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
}

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ChatApp.Realtime.Abstractions.Diagnostics;

public sealed class NatsTransportMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly ObservableGauge<long> _connectedGauge;
    private readonly Counter<long> _connectionsOpenedCounter;
    private readonly Counter<long> _reconnectionsCounter;
    private readonly Counter<long> _connectionsDisconnectedCounter;
    private readonly Counter<long> _reconnectFailuresCounter;
    private readonly Counter<long> _messagesDroppedCounter;
    private readonly Histogram<long> _droppedSubscriptionPending;
    private readonly Counter<long> _slowConsumersCounter;
    private readonly Counter<long> _serverErrorsCounter;
    private readonly Counter<long> _deliveriesCounter;
    private readonly Counter<long> _redeliveriesCounter;
    private readonly ObservableGauge<long> _pendingGauge;
    private readonly ObservableGauge<long> _ackInFlightGauge;
    private readonly Histogram<double> _ackDuration;
    private readonly Counter<long> _acknowledgementsCounter;
    private readonly Counter<long> _ackFailuresCounter;
    private readonly ConcurrentDictionary<string, long> _pendingByConsumer =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _ackInFlightByConsumer =
        new(StringComparer.Ordinal);

    private long _connected;
    private long _everConnected;
    private long _connectionsOpened;
    private long _reconnections;
    private long _connectionsDisconnected;
    private long _reconnectFailures;
    private long _messagesDropped;
    private long _slowConsumers;
    private long _serverErrors;
    private long _deliveries;
    private long _redeliveries;
    private long _acknowledgements;
    private long _ackFailures;

    public NatsTransportMetrics(string meterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meterName);
        _meter = new Meter(meterName, "1.0.0");
        _connectedGauge = _meter.CreateObservableGauge<long>(
            "chatapp.nats.connection.connected",
            () => Interlocked.Read(ref _connected));
        _connectionsOpenedCounter = _meter.CreateCounter<long>(
            "chatapp.nats.connection.opened");
        _reconnectionsCounter = _meter.CreateCounter<long>(
            "chatapp.nats.connection.reconnections");
        _connectionsDisconnectedCounter = _meter.CreateCounter<long>(
            "chatapp.nats.connection.disconnected");
        _reconnectFailuresCounter = _meter.CreateCounter<long>(
            "chatapp.nats.connection.reconnect.failures");
        _messagesDroppedCounter = _meter.CreateCounter<long>(
            "chatapp.nats.messages.dropped");
        _droppedSubscriptionPending = _meter.CreateHistogram<long>(
            "chatapp.nats.subscription.pending_at_drop");
        _slowConsumersCounter = _meter.CreateCounter<long>(
            "chatapp.nats.slow_consumers");
        _serverErrorsCounter = _meter.CreateCounter<long>(
            "chatapp.nats.server.errors");
        _deliveriesCounter = _meter.CreateCounter<long>(
            "chatapp.jetstream.deliveries");
        _redeliveriesCounter = _meter.CreateCounter<long>(
            "chatapp.jetstream.redeliveries");
        _pendingGauge = _meter.CreateObservableGauge<long>(
            "chatapp.jetstream.pending",
            () => ObserveByConsumer(_pendingByConsumer));
        _ackInFlightGauge = _meter.CreateObservableGauge<long>(
            "chatapp.jetstream.ack.in_flight",
            () => ObserveByConsumer(_ackInFlightByConsumer));
        _ackDuration = _meter.CreateHistogram<double>(
            "chatapp.jetstream.ack.duration",
            "s");
        _acknowledgementsCounter = _meter.CreateCounter<long>(
            "chatapp.jetstream.acknowledgements");
        _ackFailuresCounter = _meter.CreateCounter<long>(
            "chatapp.jetstream.ack.failures");
    }

    public void RecordConnectionOpened()
    {
        Interlocked.Exchange(ref _connected, 1);
        Interlocked.Increment(ref _connectionsOpened);
        _connectionsOpenedCounter.Add(1);
        if (Interlocked.Exchange(ref _everConnected, 1) == 0)
            return;

        Interlocked.Increment(ref _reconnections);
        _reconnectionsCounter.Add(1);
    }

    public void RecordConnectionDisconnected()
    {
        Interlocked.Exchange(ref _connected, 0);
        Interlocked.Increment(ref _connectionsDisconnected);
        _connectionsDisconnectedCounter.Add(1);
    }

    public void RecordReconnectFailure()
    {
        Interlocked.Exchange(ref _connected, 0);
        Interlocked.Increment(ref _reconnectFailures);
        _reconnectFailuresCounter.Add(1);
    }

    public void RecordMessageDropped(string? subject, int pending)
    {
        var normalizedSubject = Normalize(subject);
        Interlocked.Increment(ref _messagesDropped);
        _messagesDroppedCounter.Add(
            1,
            new KeyValuePair<string, object?>("subject", normalizedSubject));
        _droppedSubscriptionPending.Record(
            pending,
            new KeyValuePair<string, object?>("subject", normalizedSubject));
    }

    public void RecordSlowConsumer()
    {
        Interlocked.Increment(ref _slowConsumers);
        _slowConsumersCounter.Add(1);
    }

    public void RecordServerError(string? kind)
    {
        Interlocked.Increment(ref _serverErrors);
        _serverErrorsCounter.Add(
            1,
            new KeyValuePair<string, object?>("error.kind", Normalize(kind)));
    }

    public void RecordJetStreamDelivery(
        string? consumer,
        ulong deliveryCount,
        ulong pending)
    {
        var normalizedConsumer = Normalize(consumer);
        _pendingByConsumer[normalizedConsumer] = Clamp(pending);
        _ackInFlightByConsumer.AddOrUpdate(
            normalizedConsumer,
            1,
            static (_, current) => current + 1);
        Interlocked.Increment(ref _deliveries);
        _deliveriesCounter.Add(
            1,
            new KeyValuePair<string, object?>("consumer", normalizedConsumer));
        if (deliveryCount <= 1)
            return;

        Interlocked.Increment(ref _redeliveries);
        _redeliveriesCounter.Add(
            1,
            new KeyValuePair<string, object?>("consumer", normalizedConsumer));
    }

    public void RecordJetStreamAcknowledgement(
        string? consumer,
        string outcome,
        TimeSpan duration,
        bool succeeded)
    {
        var normalizedConsumer = Normalize(consumer);
        _ackInFlightByConsumer.AddOrUpdate(
            normalizedConsumer,
            0,
            static (_, current) => Math.Max(0, current - 1));
        var tags = new TagList
        {
            { "consumer", normalizedConsumer },
            { "outcome", outcome }
        };
        _ackDuration.Record(duration.TotalSeconds, tags);
        Interlocked.Increment(ref _acknowledgements);
        _acknowledgementsCounter.Add(1, tags);
        if (succeeded)
            return;

        Interlocked.Increment(ref _ackFailures);
        _ackFailuresCounter.Add(1, tags);
    }

    public NatsTransportMetricsSnapshot GetSnapshot() => new(
        Interlocked.Read(ref _connected),
        Interlocked.Read(ref _connectionsOpened),
        Interlocked.Read(ref _reconnections),
        Interlocked.Read(ref _connectionsDisconnected),
        Interlocked.Read(ref _reconnectFailures),
        Interlocked.Read(ref _messagesDropped),
        Interlocked.Read(ref _slowConsumers),
        Interlocked.Read(ref _serverErrors),
        Interlocked.Read(ref _deliveries),
        Interlocked.Read(ref _redeliveries),
        Interlocked.Read(ref _acknowledgements),
        Interlocked.Read(ref _ackFailures),
        _pendingByConsumer.Values.Sum(),
        _ackInFlightByConsumer.Values.Sum());

    public void Dispose() => _meter.Dispose();

    private static IEnumerable<Measurement<long>> ObserveByConsumer(
        ConcurrentDictionary<string, long> values)
    {
        foreach (var pair in values)
        {
            yield return new Measurement<long>(
                pair.Value,
                new KeyValuePair<string, object?>("consumer", pair.Key));
        }
    }

    private static string Normalize(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "unknown" : value;

    private static long Clamp(ulong value) =>
        value > (ulong)long.MaxValue ? long.MaxValue : (long)value;
}

public sealed record NatsTransportMetricsSnapshot(
    long Connected,
    long ConnectionsOpened,
    long Reconnections,
    long ConnectionsDisconnected,
    long ReconnectFailures,
    long MessagesDropped,
    long SlowConsumers,
    long ServerErrors,
    long Deliveries,
    long Redeliveries,
    long Acknowledgements,
    long AckFailures,
    long Pending,
    long AckInFlight);


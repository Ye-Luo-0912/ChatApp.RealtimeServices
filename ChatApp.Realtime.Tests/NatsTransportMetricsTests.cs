using ChatApp.Realtime.Abstractions.Diagnostics;

namespace ChatApp.Realtime.Tests;

public sealed class NatsTransportMetricsTests
{
    [Fact]
    public void Snapshot_TracksConnectionDeliveryAndAcknowledgementState()
    {
        using var metrics = new NatsTransportMetrics(
            $"ChatApp.Realtime.Tests.Nats.{Guid.NewGuid():N}");

        metrics.RecordConnectionOpened();
        metrics.RecordConnectionDisconnected();
        metrics.RecordReconnectFailure();
        metrics.RecordConnectionOpened();
        metrics.RecordMessageDropped("chat.events", 17);
        metrics.RecordSlowConsumer();
        metrics.RecordServerError("permissions_violation");
        metrics.RecordJetStreamDelivery("gateway-a", 1, 7);
        metrics.RecordJetStreamDelivery("gateway-a", 2, 3);
        metrics.RecordJetStreamAcknowledgement(
            "gateway-a", "ack", TimeSpan.FromMilliseconds(20), succeeded: true);
        metrics.RecordJetStreamAcknowledgement(
            "gateway-a", "nak", TimeSpan.FromMilliseconds(40), succeeded: false);

        var snapshot = metrics.GetSnapshot();

        Assert.Equal(1, snapshot.Connected);
        Assert.Equal(2, snapshot.ConnectionsOpened);
        Assert.Equal(1, snapshot.Reconnections);
        Assert.Equal(1, snapshot.ConnectionsDisconnected);
        Assert.Equal(1, snapshot.ReconnectFailures);
        Assert.Equal(1, snapshot.MessagesDropped);
        Assert.Equal(1, snapshot.SlowConsumers);
        Assert.Equal(1, snapshot.ServerErrors);
        Assert.Equal(2, snapshot.Deliveries);
        Assert.Equal(1, snapshot.Redeliveries);
        Assert.Equal(2, snapshot.Acknowledgements);
        Assert.Equal(1, snapshot.AckFailures);
        Assert.Equal(3, snapshot.Pending);
        Assert.Equal(0, snapshot.AckInFlight);
    }
}

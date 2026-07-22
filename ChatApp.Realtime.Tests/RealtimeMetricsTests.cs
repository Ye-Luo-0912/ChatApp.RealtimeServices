using System.Diagnostics.Metrics;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeMetricsTests
{
    [Fact]
    public void OutboxStats_AreReportedAsObservableGauges()
    {
        var longMeasurements = new Dictionary<string, long>();
        var doubleMeasurements = new Dictionary<string, double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == RealtimeMetrics.MeterName)
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, _, _) =>
                longMeasurements[instrument.Name] = measurement);
        listener.SetMeasurementEventCallback<double>(
            (instrument, measurement, _, _) =>
                doubleMeasurements[instrument.Name] = measurement);
        listener.Start();

        using var metrics = new RealtimeMetrics();
        metrics.UpdateOutboxStats(new RealtimeOutboxStats(
            PendingCount: 7,
            OldestPendingAtMs: DateTimeOffset.UtcNow
                .AddSeconds(-3)
                .ToUnixTimeMilliseconds(),
            MaxAttemptCount: 4));

        listener.RecordObservableInstruments();

        Assert.Equal(7, longMeasurements["realtime.outbox.pending"]);
        Assert.Equal(4, longMeasurements["realtime.outbox.max_attempts"]);
        Assert.InRange(
            doubleMeasurements["realtime.outbox.oldest.age"],
            2.9,
            10);
    }
}

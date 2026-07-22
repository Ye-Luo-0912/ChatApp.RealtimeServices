using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.State;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class ReliabilityPrimitiveTests
{
    [Fact]
    public async Task NoopStore_ThrowsInsteadOfSilentlyLosingMessage()
    {
        var store = new NoopRealtimeMessageStore(NullLogger<NoopRealtimeMessageStore>.Instance);
        var message = new RealtimeMessageRecord
        {
            MessageId = "m1",
            ClientMessageId = "c1",
            SenderUserId = 1,
            SenderSessionId = "s1",
            ReceiverUserId = 2,
            Content = "hello"
        };
        var evt = new RealtimeEvent
        {
            EventId = new string('a', 64),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 2
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(message, evt));
    }

    [Fact]
    public async Task OutboxSignal_CoalescesNotifications()
    {
        using var signal = new RealtimeOutboxSignal();

        signal.Notify();
        signal.Notify();

        Assert.True(await signal.WaitAsync(TimeSpan.Zero));
        Assert.False(await signal.WaitAsync(TimeSpan.Zero));
    }

    [Fact]
    public async Task InMemoryStateStore_RemovesExpiredEntries()
    {
        var store = new InMemoryRealtimeStateStore();
        await store.SetAsync("session:1", "online", TimeSpan.Zero);

        var value = await store.GetAsync("session:1");

        Assert.Null(value);
    }

    [Fact]
    public void Readiness_RejectsStaleHeartbeat()
    {
        var state = new RealtimeReadinessState();
        state.MarkStarted("worker");

        var snapshot = state.GetSnapshot(TimeSpan.FromMilliseconds(-1));

        Assert.False(snapshot.IsReady);
    }

    [Fact]
    public void HistoryMetricsTrackQueueAndInFlightLifecycle()
    {
        using var metrics = new RealtimeMetrics();

        metrics.HistoryQueryEnqueued();
        var queued = metrics.GetSnapshot();
        Assert.Equal(1, queued.HistoryQueryQueueDepth);
        Assert.Equal(0, queued.HistoryQueriesInFlight);

        metrics.HistoryQueryStarted();
        var started = metrics.GetSnapshot();
        Assert.Equal(0, started.HistoryQueryQueueDepth);
        Assert.Equal(1, started.HistoryQueriesInFlight);

        metrics.RecordHistoryQuery(
            succeeded: true,
            reason: null,
            TimeSpan.FromMilliseconds(3));
        var completed = metrics.GetSnapshot();
        Assert.Equal(0, completed.HistoryQueryQueueDepth);
        Assert.Equal(0, completed.HistoryQueriesInFlight);
        Assert.Equal(1, completed.HistoryQueries);
    }
}

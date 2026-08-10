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
    public void OutboxSignal_BoundsCommittedHintsAndPreservesOrder()
    {
        using var signal = new RealtimeOutboxSignal(hintCapacity: 2);
        var hints = (IRealtimeOutboxHintSource)signal;

        signal.Notify("event-1");
        signal.Notify("event-2");
        signal.Notify("event-overflow");

        Assert.True(hints.TryReadCommittedEventId(out var first));
        Assert.True(hints.TryReadCommittedEventId(out var second));
        Assert.False(hints.TryReadCommittedEventId(out _));
        Assert.Equal("event-1", first);
        Assert.Equal("event-2", second);
    }

    [Fact]
    public void OutboxSignal_PreclaimReservationIsBoundedAndCommitCarriesOwnership()
    {
        using var signal = new RealtimeOutboxSignal(hintCapacity: 1);
        var coordinator = (IRealtimeOutboxPreclaimCoordinator)signal;
        var hints = (IRealtimeOutboxHintSource)signal;
        coordinator.ConfigurePreclaimOwner("publisher-1", TimeSpan.FromSeconds(30));

        Assert.True(coordinator.TryReservePreclaim("event-cancel", out var cancelled));
        Assert.False(coordinator.TryReservePreclaim("event-overflow", out _));
        coordinator.CancelPreclaim(cancelled);

        Assert.True(coordinator.TryReservePreclaim("event-commit", out var committed));
        coordinator.CommitPreclaim(committed);

        Assert.True(hints.TryReadCommittedHint(out var hint));
        Assert.True(hint.IsPreclaimed);
        Assert.Equal(committed.EventId, hint.EventId);
        Assert.Equal(committed.LockOwner, hint.LockOwner);
        Assert.Equal(committed.ClaimToken, hint.ClaimToken);
        Assert.False(hints.TryReadCommittedHint(out _));
    }

    [Fact]
    public void OutboxSignal_PreclaimsReusePublisherGenerationToken()
    {
        using var signal = new RealtimeOutboxSignal(hintCapacity: 3);
        var coordinator = (IRealtimeOutboxPreclaimCoordinator)signal;
        coordinator.ConfigurePreclaimOwner("publisher-1", TimeSpan.FromSeconds(30));

        Assert.True(coordinator.TryReservePreclaim("event-1", out var first));
        Assert.True(coordinator.TryReservePreclaim("event-2", out var second));
        Assert.Equal(first.ClaimToken, second.ClaimToken);

        coordinator.ConfigurePreclaimOwner("publisher-2", TimeSpan.FromSeconds(30));
        Assert.True(coordinator.TryReservePreclaim("event-3", out var nextGeneration));
        Assert.NotEqual(first.ClaimToken, nextGeneration.ClaimToken);
        Assert.Equal("publisher-2", nextGeneration.LockOwner);

        coordinator.CancelPreclaim(first);
        coordinator.CancelPreclaim(second);
        coordinator.CancelPreclaim(nextGeneration);
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

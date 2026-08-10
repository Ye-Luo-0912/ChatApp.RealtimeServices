using ChatApp.RealtimeServices.Workers;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class OutboxHintCoalescingTests
{
    [Fact]
    public void DisabledWindow_DoesNotDelay()
    {
        var delay = OutboxPublisherWorker.CalculateHintCoalescingDelayMilliseconds(
            configuredWindowMs: 0,
            bufferedHintCount: 1,
            batchSize: 100,
            nextRecoveryScanAt: 10_000,
            nextCompletionFlushAt: 0,
            now: 1_000);

        Assert.Equal(0, delay);
    }

    [Fact]
    public void PartialBatch_UsesConfiguredBoundedWindow()
    {
        var delay = OutboxPublisherWorker.CalculateHintCoalescingDelayMilliseconds(
            configuredWindowMs: 3,
            bufferedHintCount: 1,
            batchSize: 100,
            nextRecoveryScanAt: 10_000,
            nextCompletionFlushAt: 2_000,
            now: 1_000);

        Assert.Equal(3, delay);
    }

    [Theory]
    [InlineData(100, 100, 10_000, 0)]
    [InlineData(1, 100, 1_003, 0)]
    [InlineData(1, 100, 10_000, 1_003)]
    public void FullBatchOrDueDeadline_DoesNotDelay(
        int bufferedHintCount,
        int batchSize,
        long nextRecoveryScanAt,
        long nextCompletionFlushAt)
    {
        var delay = OutboxPublisherWorker.CalculateHintCoalescingDelayMilliseconds(
            configuredWindowMs: 3,
            bufferedHintCount,
            batchSize,
            nextRecoveryScanAt,
            nextCompletionFlushAt,
            now: 1_000);

        Assert.Equal(0, delay);
    }
}

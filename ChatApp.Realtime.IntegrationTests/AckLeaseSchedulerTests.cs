using System.Diagnostics;
using ChatApp.RealtimeServices.Workers.Reliability;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class AckLeaseSchedulerTests
{
    [Fact]
    public void Start_ReturnsNull_WhenAckWaitIsZero()
    {
        Assert.Null(
            AckLeaseScheduler.Start(
                TimeSpan.Zero,
                NullLogger.Instance));
    }

    [Fact]
    public async Task FastMessage_DoesNotEmitProgressAck()
    {
        var progressAckCount = 0;
        var scheduler = AckLeaseScheduler.Start(
            TimeSpan.FromSeconds(1),
            NullLogger.Instance);
        Assert.NotNull(scheduler);

        var lease = scheduler.Register(
            _ =>
            {
                Interlocked.Increment(ref progressAckCount);
                return ValueTask.CompletedTask;
            },
            CancellationToken.None);

        // 快消息立即完成：在 AckWait/2 = 500ms 到期前 Complete。
        lease.Complete();

        await Task.Delay(1_200);
        await scheduler.DisposeAsync();

        Assert.Equal(0, progressAckCount);
    }

    [Fact]
    public async Task SlowMessage_ReceivesProgressAckBeforeExpiry()
    {
        var progressAckCount = 0;
        var scheduler = AckLeaseScheduler.Start(
            TimeSpan.FromSeconds(1),
            NullLogger.Instance);
        Assert.NotNull(scheduler);

        var lease = scheduler.Register(
            _ =>
            {
                Interlocked.Increment(ref progressAckCount);
                return ValueTask.CompletedTask;
            },
            CancellationToken.None);

        // 慢消息：保持活跃超过 AckWait/2，等待调度器发出 progress-ack。
        await WaitUntilAsync(() => Volatile.Read(ref progressAckCount) > 0, TimeSpan.FromSeconds(3));
        lease.Complete();

        await WaitUntilAsync(() => Volatile.Read(ref progressAckCount) >= 1, TimeSpan.FromSeconds(1));
        await scheduler.DisposeAsync();

        Assert.True(progressAckCount >= 1, "慢消息应在 AckWait 到期前收到至少一次 progress-ack。");
    }

    [Fact]
    public async Task Register_AfterDispose_ReturnsInactiveLease()
    {
        var scheduler = AckLeaseScheduler.Start(
            TimeSpan.FromSeconds(1),
            NullLogger.Instance);
        Assert.NotNull(scheduler);
        await scheduler.DisposeAsync();

        var lease = scheduler.Register(
            _ => ValueTask.CompletedTask,
            CancellationToken.None);

        Assert.False(lease.IsActive);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition() && stopwatch.Elapsed < timeout)
            await Task.Delay(50);
    }
}
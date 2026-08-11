using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1：<see cref="AttachmentSweepWorker"/> 后台服务测试。验证启用时周期调用
/// <see cref="IAttachmentSweeper.SweepAsync"/>、停用时保持空闲、单轮异常不阻断后续周期。
/// </summary>
public sealed class AttachmentSweepWorkerTests
{
    private const int IntervalMs = 1_000;

    [Fact]
    public async Task RunAsync_WhenEnabled_CallsSweepAsync()
    {
        var sweeper = new RecordingSweeper();
        using var worker = new AttachmentSweepWorker(
            sweeper,
            Options.Create(new AttachmentSweepOptions
            {
                Enabled = true,
                IntervalMs = IntervalMs,
                RetentionDays = 7
            }),
            NullLogger<AttachmentSweepWorker>.Instance);

        int Count() => Volatile.Read(ref sweeper.CallCountRef);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            await WaitUntilAsync(() => Count() > 0, TimeSpan.FromSeconds(3));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.True(Count() > 0, "启用状态下 worker 应周期调用 SweepAsync。");
    }

    [Fact]
    public async Task RunAsync_WhenDisabled_DoesNotCallSweep()
    {
        var sweeper = new RecordingSweeper();
        using var worker = new AttachmentSweepWorker(
            sweeper,
            Options.Create(new AttachmentSweepOptions
            {
                Enabled = false,
                IntervalMs = IntervalMs,
                RetentionDays = 7
            }),
            NullLogger<AttachmentSweepWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            // 跨多个周期等待，停用状态下不应有任何调用。
            await Task.Delay(TimeSpan.FromMilliseconds(IntervalMs + 1_200));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.Equal(0, sweeper.CallCountRef);
    }

    [Fact]
    public async Task RunAsync_SweepFailure_DoesNotStopWorker()
    {
        var sweeper = new RecordingSweeper(throwOnFirst: true);
        using var worker = new AttachmentSweepWorker(
            sweeper,
            Options.Create(new AttachmentSweepOptions
            {
                Enabled = true,
                IntervalMs = IntervalMs,
                RetentionDays = 7
            }),
            NullLogger<AttachmentSweepWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            // 首轮抛异常后，worker 不应停止，后续周期应继续成功调用。
            await WaitUntilAsync(() => Volatile.Read(ref sweeper.CallCountRef) >= 2, TimeSpan.FromSeconds(4));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.True(sweeper.CallCountRef >= 2, "单轮异常后 worker 应继续后续周期。");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(10);
        }

        throw new TimeoutException("等待条件超时。");
    }

    private sealed class RecordingSweeper : IAttachmentSweeper
    {
        private readonly bool _throwOnFirst;

        public RecordingSweeper(bool throwOnFirst = false)
        {
            _throwOnFirst = throwOnFirst;
        }

        public int CallCountRef;

        public Task<int> SweepAsync(CancellationToken ct = default)
        {
            var call = Interlocked.Increment(ref CallCountRef);
            if (_throwOnFirst && call == 1)
                throw new InvalidOperationException("模拟首轮清理失败。");
            return Task.FromResult(1);
        }
    }
}
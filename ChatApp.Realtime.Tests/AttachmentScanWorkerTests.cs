using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.RealtimeServices.Concurrency;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1：<see cref="AttachmentScanWorker"/> 后台服务测试。验证消费扫描命令并调用
/// <see cref="IAttachmentScanProcessor.ProcessAsync"/>、单命令异常/失败不阻断消费循环、
/// 以及共享并发门过载时跳过命令（fire-and-forget，命令可重放）。
/// </summary>
public sealed class AttachmentScanWorkerTests
{
    [Fact]
    public async Task RunAsync_ConsumesAndProcessesCommand()
    {
        var (opts, gate) = CreateHarness();
        var cmd = NewCommand("r1");
        AttachmentScanCommand? processed = null;
        var processor = new RecordingScanProcessor(c =>
        {
            processed = c;
            return Task.FromResult(
                AttachmentScanResult.Success(c.RequestId, c.AttachmentId, 7));
        });
        var consumer = new StubScanConsumer(cmd);
        using var worker = new AttachmentScanWorker(
            consumer,
            processor,
            new RealtimeReadinessState(),
            Options.Create(opts),
            gate,
            NullLogger<AttachmentScanWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            await WaitUntilAsync(() => processed is not null, TimeSpan.FromSeconds(3));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.Same(cmd, processed);
        Assert.Single(processor.Received);
    }

    [Fact]
    public async Task RunAsync_ProcessorThrows_DoesNotStopWorker()
    {
        var (opts, gate) = CreateHarness();
        var count = 0;
        var processor = new RecordingScanProcessor(c =>
        {
            var n = Interlocked.Increment(ref count);
            if (n == 1)
                throw new InvalidOperationException("模拟扫描异常。");
            return Task.FromResult(
                AttachmentScanResult.Success(c.RequestId, c.AttachmentId, 7));
        });
        // 首条抛异常后，worker 不应停止，下一条应继续被处理。
        var consumer = new StubScanConsumer(NewCommand("r1"), NewCommand("r2"));
        using var worker = new AttachmentScanWorker(
            consumer,
            processor,
            new RealtimeReadinessState(),
            Options.Create(opts),
            gate,
            NullLogger<AttachmentScanWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            await WaitUntilAsync(
                () => Volatile.Read(ref count) >= 2,
                TimeSpan.FromSeconds(3));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.True(Volatile.Read(ref count) >= 2, "单命令异常后 worker 应继续处理后续命令。");
    }

    [Fact]
    public async Task RunAsync_ProcessorFailedResult_ContinuesToNext()
    {
        var (opts, gate) = CreateHarness();
        var count = 0;
        var processor = new RecordingScanProcessor(c =>
        {
            var n = Interlocked.Increment(ref count);
            return Task.FromResult(
                n == 1
                    ? AttachmentScanResult.Failed(c.RequestId, "scan_failed", "模拟失败。")
                    : AttachmentScanResult.Success(c.RequestId, c.AttachmentId, 7));
        });
        var consumer = new StubScanConsumer(NewCommand("r1"), NewCommand("r2"));
        using var worker = new AttachmentScanWorker(
            consumer,
            processor,
            new RealtimeReadinessState(),
            Options.Create(opts),
            gate,
            NullLogger<AttachmentScanWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            await WaitUntilAsync(
                () => Volatile.Read(ref count) >= 2,
                TimeSpan.FromSeconds(3));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.True(Volatile.Read(ref count) >= 2, "返回 Failed 结果后 worker 应继续处理后续命令。");
    }

    [Fact]
    public async Task RunAsync_WhenGateSaturated_SkipsCommandWithoutProcessing()
    {
        var (opts, gate) = CreateHarness(overloadTimeoutMs: 50);
        // 饱和变更并发池：worker 在超时内无法获许，应跳过命令（fire-and-forget）。
        await gate.WaitAsync(QueryPoolKind.Mutation, 0, CancellationToken.None);

        var count = 0;
        var processor = new RecordingScanProcessor(c =>
        {
            Interlocked.Increment(ref count);
            return Task.FromResult(
                AttachmentScanResult.Success(c.RequestId, c.AttachmentId, 7));
        });
        // 无限流消费者：饱和期间全部跳过；释放后第一条被处理。
        var consumer = new LoopScanConsumer(() => NewCommand("r1"));
        using var worker = new AttachmentScanWorker(
            consumer,
            processor,
            new RealtimeReadinessState(),
            Options.Create(opts),
            gate,
            NullLogger<AttachmentScanWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);

        try
        {
            // 饱和度保持期间，命令应被跳过（不调用 processor）。
            await Task.Delay(TimeSpan.FromMilliseconds(300));
            Assert.Equal(0, Volatile.Read(ref count));

            // 释放并发许可后，命令应被处理。
            gate.Release(QueryPoolKind.Mutation);
            await WaitUntilAsync(
                () => Volatile.Read(ref count) >= 1,
                TimeSpan.FromSeconds(3));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        Assert.True(Volatile.Read(ref count) >= 1, "并发许可释放后 worker 应处理命令。");
    }

    private static (RealtimeOptions Options, RealtimeQueryConcurrencyGate Gate) CreateHarness(
        int overloadTimeoutMs = 200)
    {
        var opts = new RealtimeOptions
        {
            ServiceName = "test",
            InstanceId = "test",
            MutationQueryConcurrency = 1,
            OverloadGateTimeoutMs = overloadTimeoutMs
        };
        return (opts, new RealtimeQueryConcurrencyGate(Options.Create(opts)));
    }

    private static AttachmentScanCommand NewCommand(string requestId) => new()
    {
        RequestId = requestId,
        AttachmentId = "att-" + requestId,
        Verdict = AttachmentScanVerdict.Pass,
        StateVersion = 1,
        SizeBytes = 100
    };

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

    private sealed class StubScanConsumer : IAttachmentScanConsumer
    {
        private readonly AttachmentScanCommand[] _commands;

        public StubScanConsumer(params AttachmentScanCommand[] commands)
        {
            _commands = commands;
        }

        public async IAsyncEnumerable<AttachmentScanCommand> ConsumeAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (var command in _commands)
            {
                await Task.Yield();
                yield return command;
            }
        }
    }

    private sealed class LoopScanConsumer : IAttachmentScanConsumer
    {
        private readonly Func<AttachmentScanCommand> _factory;

        public LoopScanConsumer(Func<AttachmentScanCommand> factory)
        {
            _factory = factory;
        }

        public async IAsyncEnumerable<AttachmentScanCommand> ConsumeAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Yield();
                yield return _factory();
            }
        }
    }

    private sealed class RecordingScanProcessor : IAttachmentScanProcessor
    {
        private readonly Func<AttachmentScanCommand, Task<AttachmentScanResult>> _handler;

        public RecordingScanProcessor(
            Func<AttachmentScanCommand, Task<AttachmentScanResult>> handler)
        {
            _handler = handler;
        }

        public ConcurrentQueue<AttachmentScanCommand> Received { get; } = new();

        public async Task<AttachmentScanResult> ProcessAsync(
            AttachmentScanCommand command,
            CancellationToken ct = default)
        {
            Received.Enqueue(command);
            return await _handler(command);
        }
    }
}
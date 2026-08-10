using ChatApp.Realtime.Infrastructure.Core.Concurrency;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using System.Text.Json;

namespace ChatApp.Realtime.Tests;

public sealed class SharedConcurrencyPrimitiveTests
{
    [Fact]
    public async Task CoalescingSignal_ConcurrentNotificationsProduceSingleWake()
    {
        using var signal = new CoalescingAsyncSignal();

        Parallel.For(0, 10_000, _ => signal.Notify());

        Assert.True(await signal.WaitAsync(TimeSpan.Zero));
        Assert.False(await signal.WaitAsync(TimeSpan.Zero));
    }

    [Fact]
    public async Task LeaseScheduler_FastCompletionDoesNotRunRenewal()
    {
        var renewals = 0;
        await using var scheduler = new SharedLeaseScheduler<int>(
            TimeSpan.FromMilliseconds(50),
            (_, _) =>
            {
                Interlocked.Increment(ref renewals);
                return ValueTask.FromResult(true);
            });

        var registration = scheduler.Register(1);
        registration.Complete();

        await Task.Delay(150);
        Assert.Equal(0, Volatile.Read(ref renewals));
    }

    [Fact]
    public async Task LeaseScheduler_ReusesNodeWithoutAllowingStaleCompletion()
    {
        var renewals = 0;
        await using var scheduler = new SharedLeaseScheduler<int>(
            TimeSpan.FromMilliseconds(30),
            (_, _) =>
            {
                Interlocked.Increment(ref renewals);
                return ValueTask.FromResult(true);
            });

        var first = scheduler.Register(1);
        first.Complete();

        var second = scheduler.Register(2);
        first.Complete();
        await WaitUntilAsync(() => Volatile.Read(ref renewals) > 0, TimeSpan.FromSeconds(2));
        second.Complete();

        Assert.True(Volatile.Read(ref renewals) > 0);
    }

    [Fact]
    public async Task PooledBuffer_StaleLeaseCannotReturnReusedWriterAcrossThreads()
    {
        // 占用另一个线程缓存槽，确保 first 归还后由 second 取回同一 Writer。
        using var guard = PooledByteBufferWriter.Rent();
        var first = PooledByteBufferWriter.Rent();
        var originalWriter = first.BufferWriter;
        first.BufferWriter.GetSpan(1)[0] = 99;
        first.BufferWriter.Advance(1);
        first.Dispose();

        using var second = PooledByteBufferWriter.Rent();
        Assert.Same(originalWriter, second.BufferWriter);
        Assert.Equal(0, second.BufferWriter.GetSpan(1)[0]);

        // 模拟旧调用方误二次释放。版本令牌必须保护当前租约。
        await Task.Run(first.Dispose);
        var span = second.BufferWriter.GetSpan(1);
        span[0] = 42;
        second.BufferWriter.Advance(1);

        Assert.Equal(new byte[] { 42 }, second.ToArray());
    }

    [Fact]
    public async Task LeaseScheduler_ExposesRejectedRenewalUntilCompletion()
    {
        await using var scheduler = new SharedLeaseScheduler<int>(
            TimeSpan.FromMilliseconds(30),
            (_, _) => ValueTask.FromResult(false));

        var registration = scheduler.Register(1);
        await WaitUntilAsync(() => registration.RenewalRejected, TimeSpan.FromSeconds(2));

        Assert.True(registration.RenewalRejected);
        registration.Complete();
        Assert.False(registration.IsActive);
    }

    [Fact]
    public void RealtimeWireSerializer_TypedPayloadPreservesExistingWireContract()
    {
        var payload = new RealtimeChatMessagePayload
        {
            MessageId = "message-1",
            ClientMessageId = "client-1",
            SenderUserId = 10,
            SenderSessionId = "session-1",
            ReceiverUserId = 20,
            ConversationId = "dm:10:20",
            Content = "包含引号 \"、反斜线 \\ 和 emoji 🚀",
            ReceivedAtMs = 1_700_000_000_000
        };
        var payloadJson = JsonSerializer.Serialize(
            payload,
            RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
        var expected = new RealtimeEvent
        {
            EventId = "event-1",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 20,
            ActorUserId = 10,
            MessageId = "message-1",
            SessionId = "session-1",
            PayloadJson = payloadJson,
            OccurredAtMs = 1_700_000_000_000
        };
        var optimized = new RealtimeEvent
        {
            EventId = expected.EventId,
            Type = expected.Type,
            TargetUserId = expected.TargetUserId,
            ActorUserId = expected.ActorUserId,
            MessageId = expected.MessageId,
            SessionId = expected.SessionId,
            Payload = payload,
            OccurredAtMs = expected.OccurredAtMs
        };

        var expectedBytes = JsonSerializer.SerializeToUtf8Bytes(
            expected,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);
        var actualBytes = RealtimeEventWireSerializer.SerializeToUtf8Bytes(optimized);

        using var expectedJson = JsonDocument.Parse(expectedBytes);
        using var actualJson = JsonDocument.Parse(actualBytes);
        Assert.True(JsonElement.DeepEquals(expectedJson.RootElement, actualJson.RootElement));
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(10);
    }
}

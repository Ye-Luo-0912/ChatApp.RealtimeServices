using System.Text.Json;
using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 对比旧的 Payload UTF-16 字符串中间态与共享 UTF-8 缓冲快路径。
/// 输出 byte[] 是事务写入 Outbox 必须持有的最终载荷，不计作可消除的临时缓冲。
/// </summary>
[MemoryDiagnoser]
public class RealtimeWireSerializationBenchmarks
{
    private readonly RealtimeChatMessagePayload _payload = new()
    {
        MessageId = "benchmark-message",
        ClientMessageId = "benchmark-client-message",
        SenderUserId = 10,
        SenderSessionId = "benchmark-session",
        ReceiverUserId = 20,
        ConversationId = "dm:10:20",
        Content = new string('x', 512),
        ConversationSequence = 42,
        ReceivedAtMs = 1_700_000_000_000
    };

    [Benchmark(Baseline = true, Description = "Wire JSON via PayloadJson UTF-16 intermediate")]
    public byte[] LegacyPayloadStringThenEnvelope()
    {
        var payloadJson = JsonSerializer.Serialize(
            _payload,
            RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload);
        return JsonSerializer.SerializeToUtf8Bytes(
            CreateEvent(payloadJson, payload: null),
            RealtimeJsonSerializerContext.Default.RealtimeEvent);
    }

    [Benchmark(Description = "Wire JSON via versioned shared UTF-8 buffers")]
    public byte[] SharedUtf8Buffers() =>
        RealtimeEventWireSerializer.SerializeToUtf8Bytes(
            CreateEvent(payloadJson: null, _payload));

    private static RealtimeEvent CreateEvent(
        string? payloadJson,
        object? payload) => new()
    {
        EventId = "benchmark-event",
        Type = RealtimeEventType.MessageReceived,
        TargetUserId = 20,
        ActorUserId = 10,
        MessageId = "benchmark-message",
        SessionId = "benchmark-session",
        PayloadJson = payloadJson,
        Payload = payload,
        OccurredAtMs = 1_700_000_000_000
    };
}

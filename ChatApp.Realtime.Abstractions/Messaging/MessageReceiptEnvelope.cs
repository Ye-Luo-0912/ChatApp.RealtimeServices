using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageReceiptEnvelope
{
    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask>? _nak;
    private readonly Func<CancellationToken, ValueTask>? _progressAck;

    [SetsRequiredMembers]
    public MessageReceiptEnvelope(MessageReceiptCommand command)
    {
        Command = command;
    }

    [SetsRequiredMembers]
    public MessageReceiptEnvelope(
        MessageReceiptCommand command,
        Func<CancellationToken, ValueTask> ack,
        Func<TimeSpan?, CancellationToken, ValueTask> nak,
        ulong? deliveryCount = null,
        string? rawPayload = null,
        ActivityContext parentContext = default,
        long? trustedUserId = null,
        Func<CancellationToken, ValueTask>? progressAck = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
        _progressAck = progressAck;
    }

    public required MessageReceiptCommand Command { get; init; }
    public ulong? DeliveryCount { get; init; }
    public string? RawPayload { get; set; }
    public ActivityContext ParentContext { get; init; }
    public long? TrustedUserId { get; init; }

    /// <summary>
    /// Perf-6：成功解析并处理后清除原始 payload，避免在队列中长期保留完整 RawPayload。
    /// </summary>
    public void ClearRawPayload() => RawPayload = null;

    public ValueTask AckAsync(CancellationToken ct = default) =>
        _ack is not null ? _ack(ct) : ValueTask.CompletedTask;

    /// <summary>
    /// Reliability-4：In-Progress ACK（JetStream WPI）。重置 AckWait 计时器，
    /// 防止长时处理消息在完成前被 JetStream 重投。无回调时为空操作。
    /// </summary>
    public ValueTask ProgressAckAsync(CancellationToken ct = default) =>
        _progressAck is not null ? _progressAck(ct) : ValueTask.CompletedTask;

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default) =>
        _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;
}

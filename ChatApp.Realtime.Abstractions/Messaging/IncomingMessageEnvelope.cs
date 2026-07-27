using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class IncomingMessageEnvelope
{
    public required IncomingMessageCommand Command { get; init; }
    public ulong? DeliveryCount { get; init; }
    public ActivityContext ParentContext { get; init; }
    /// <summary>网关身份头中的用户编号（可信）；未注入时为 null。</summary>
    public long? TrustedUserId { get; init; }
    /// <summary>网关身份头中的会话编号（可信）；未注入时为 null。</summary>
    public string? TrustedSessionId { get; init; }

    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask>? _nak;
    private readonly Func<CancellationToken, ValueTask>? _progressAck;

    [SetsRequiredMembers]
    public IncomingMessageEnvelope(IncomingMessageCommand command)
    {
        Command = command;
    }

    [SetsRequiredMembers]
    public IncomingMessageEnvelope(
        IncomingMessageCommand command,
        Func<CancellationToken, ValueTask> ack,
        Func<TimeSpan?, CancellationToken, ValueTask> nak,
        ulong? deliveryCount = null,
        string? rawPayload = null,
        ActivityContext parentContext = default,
        long? trustedUserId = null,
        string? trustedSessionId = null,
        Func<CancellationToken, ValueTask>? progressAck = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
        TrustedSessionId = trustedSessionId;
        _progressAck = progressAck;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
    {
        return _ack is not null ? _ack(ct) : ValueTask.CompletedTask;
    }

    /// <summary>
    /// Reliability-4：In-Progress ACK（JetStream WPI）。重置 AckWait 计时器，
    /// 防止长时处理消息在完成前被 JetStream 重投。无回调时为空操作。
    /// </summary>
    public ValueTask ProgressAckAsync(CancellationToken ct = default)
    {
        return _progressAck is not null ? _progressAck(ct) : ValueTask.CompletedTask;
    }

    public string? RawPayload { get; set; }

    /// <summary>
    /// Perf-6：成功解析并处理后清除原始 payload，避免在队列中长期保留完整 RawPayload。
    /// 死信路径会在需要时回退到 Command 重新序列化（截断）。
    /// </summary>
    public void ClearRawPayload() => RawPayload = null;

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default)
    {
        return _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;
    }
}

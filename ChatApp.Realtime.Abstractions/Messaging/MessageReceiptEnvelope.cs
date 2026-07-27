using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageReceiptEnvelope
{
    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask>? _nak;

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
        long? trustedUserId = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
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

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default) =>
        _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;
}

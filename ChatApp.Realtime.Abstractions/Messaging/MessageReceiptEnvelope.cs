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
        ActivityContext parentContext = default)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
    }

    public required MessageReceiptCommand Command { get; init; }
    public ulong? DeliveryCount { get; init; }
    public string? RawPayload { get; init; }
    public ActivityContext ParentContext { get; init; }

    public ValueTask AckAsync(CancellationToken ct = default) =>
        _ack is not null ? _ack(ct) : ValueTask.CompletedTask;

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default) =>
        _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;
}
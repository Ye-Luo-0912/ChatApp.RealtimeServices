using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class IncomingMessageEnvelope
{
    public required IncomingMessageCommand Command { get; init; }
    public ulong? DeliveryCount { get; init; }
    public ActivityContext ParentContext { get; init; }

    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask>? _nak;

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
        ActivityContext parentContext = default)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
    {
        return _ack is not null ? _ack(ct) : ValueTask.CompletedTask;
    }

    public string? RawPayload { get; init; }

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default)
    {
        return _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;
    }
}

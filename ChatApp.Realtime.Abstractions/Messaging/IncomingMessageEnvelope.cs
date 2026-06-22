using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class IncomingMessageEnvelope
{
    public required IncomingMessageCommand Command { get; init; }
    public ulong? DeliveryCount { get; init; }

    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<CancellationToken, ValueTask>? _nak;

    [SetsRequiredMembers]
    public IncomingMessageEnvelope(IncomingMessageCommand command)
    {
        Command = command;
    }

    [SetsRequiredMembers]
    public IncomingMessageEnvelope(
        IncomingMessageCommand command,
        Func<CancellationToken, ValueTask> ack,
        Func<CancellationToken, ValueTask> nak,
        ulong? deliveryCount = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
    {
        return _ack is not null ? _ack(ct) : ValueTask.CompletedTask;
    }

    public ValueTask NakAsync(CancellationToken ct = default)
    {
        return _nak is not null ? _nak(ct) : ValueTask.CompletedTask;
    }
}

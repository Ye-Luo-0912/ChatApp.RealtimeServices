using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Events;

public sealed class RealtimeEventEnvelope
{
    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<CancellationToken, ValueTask>? _nak;

    public required RealtimeEvent Event { get; init; }
    public ulong? DeliveryCount { get; init; }

    [SetsRequiredMembers]
    public RealtimeEventEnvelope(RealtimeEvent evt)
    {
        Event = evt;
    }

    [SetsRequiredMembers]
    public RealtimeEventEnvelope(
        RealtimeEvent evt,
        Func<CancellationToken, ValueTask> ack,
        Func<CancellationToken, ValueTask> nak,
        ulong? deliveryCount = null)
    {
        Event = evt;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
        => _ack is null ? ValueTask.CompletedTask : _ack(ct);

    public ValueTask NakAsync(CancellationToken ct = default)
        => _nak is null ? ValueTask.CompletedTask : _nak(ct);
}

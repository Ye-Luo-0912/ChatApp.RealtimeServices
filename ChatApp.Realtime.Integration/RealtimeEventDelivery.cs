using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Integration;

public sealed class RealtimeEventDelivery
{
    private readonly Func<CancellationToken, ValueTask> _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask> _nak;

    public RealtimeEventDelivery(
        RealtimeEvent evt,
        Func<CancellationToken, ValueTask> ack,
        Func<TimeSpan?, CancellationToken, ValueTask> nak,
        ulong? deliveryCount,
        ActivityContext parentContext = default)
    {
        Event = evt;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        ParentContext = parentContext;
    }

    public RealtimeEvent Event { get; }
    public ulong? DeliveryCount { get; }
    public ActivityContext ParentContext { get; }

    public ValueTask AckAsync(CancellationToken ct = default) => _ack(ct);
    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default) => _nak(delay, ct);
}

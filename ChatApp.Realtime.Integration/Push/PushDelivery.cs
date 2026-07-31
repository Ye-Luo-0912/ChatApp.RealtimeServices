using System.Diagnostics;

namespace ChatApp.Realtime.Integration.Push;

public sealed class PushDelivery
{
    private readonly Func<CancellationToken, ValueTask> _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask> _nak;

    public PushDelivery(
        PushDeliveryCommand command,
        ulong? deliveryCount,
        ActivityContext parentContext,
        Func<CancellationToken, ValueTask> ack,
        Func<TimeSpan?, CancellationToken, ValueTask> nak)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        ParentContext = parentContext;
    }

    public PushDeliveryCommand Command { get; }
    public ulong? DeliveryCount { get; }
    public ActivityContext ParentContext { get; }

    public ValueTask AckAsync(CancellationToken ct = default) => _ack(ct);
    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default) => _nak(delay, ct);
}

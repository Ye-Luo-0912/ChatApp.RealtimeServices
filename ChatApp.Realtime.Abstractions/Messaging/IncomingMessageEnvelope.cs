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
        string? trustedSessionId = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        DeliveryCount = deliveryCount;
        RawPayload = rawPayload;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
        TrustedSessionId = trustedSessionId;
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

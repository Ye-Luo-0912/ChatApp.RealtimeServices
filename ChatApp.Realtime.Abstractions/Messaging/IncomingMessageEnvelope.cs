using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class IncomingMessageEnvelope
{
    public required IncomingMessageCommand Command { get; init; }

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
        Func<CancellationToken, ValueTask> nak)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
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

public static class IncomingMessageEnvelopeExtensions
{
    public static async ValueTask TryAckAsync(
        this IncomingMessageEnvelope envelope,
        CancellationToken ct = default)
    {
        try
        {
            await envelope.AckAsync(ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }

    public static async ValueTask TryNakAsync(
        this IncomingMessageEnvelope envelope,
        CancellationToken ct = default)
    {
        try
        {
            await envelope.NakAsync(ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }
}

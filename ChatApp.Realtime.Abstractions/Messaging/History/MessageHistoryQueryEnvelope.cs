using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed class MessageHistoryQueryEnvelope
{
    private readonly Func<MessageHistoryPage, CancellationToken, ValueTask> _reply;

    [SetsRequiredMembers]
    public MessageHistoryQueryEnvelope(
        MessageHistoryQuery query,
        Func<MessageHistoryPage, CancellationToken, ValueTask> reply,
        ActivityContext parentContext = default)
    {
        Query = query;
        _reply = reply;
        ParentContext = parentContext;
    }

    public required MessageHistoryQuery Query { get; init; }
    public ActivityContext ParentContext { get; init; }

    public ValueTask ReplyAsync(
        MessageHistoryPage page,
        CancellationToken ct = default) => _reply(page, ct);
}

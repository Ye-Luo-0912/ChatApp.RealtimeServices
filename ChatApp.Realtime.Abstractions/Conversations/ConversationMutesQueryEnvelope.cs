using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationMutesQueryEnvelope(
    ConversationMutesQuery query,
    Func<ConversationMutesQueryResult, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public ConversationMutesQuery Query { get; } = query;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(ConversationMutesQueryResult result, CancellationToken ct = default) =>
        replyAsync(result, ct);
}

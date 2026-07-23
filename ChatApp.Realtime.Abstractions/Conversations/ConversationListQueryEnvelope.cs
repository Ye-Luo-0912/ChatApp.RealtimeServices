using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationListQueryEnvelope(
    ConversationListQuery query,
    Func<ConversationListPage, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public ConversationListQuery Query { get; } = query;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(ConversationListPage page, CancellationToken ct = default) =>
        replyAsync(page, ct);
}

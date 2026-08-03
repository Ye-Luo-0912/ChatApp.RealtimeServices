using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Relationships;

public sealed class RelationshipListQueryEnvelope
{
    public RelationshipListQueryEnvelope(
        RelationshipListQuery query,
        Func<RelationshipListResult, CancellationToken, ValueTask> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Query = query;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public RelationshipListQuery Query { get; }
    public Func<RelationshipListResult, CancellationToken, ValueTask> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

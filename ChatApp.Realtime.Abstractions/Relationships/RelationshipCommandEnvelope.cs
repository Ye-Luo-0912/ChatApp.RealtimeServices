using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Relationships;

public sealed class RelationshipCommandEnvelope
{
    public RelationshipCommandEnvelope(
        RelationshipCommand command,
        Func<RelationshipCommandResult, CancellationToken, ValueTask> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Command = command;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public RelationshipCommand Command { get; }
    public Func<RelationshipCommandResult, CancellationToken, ValueTask> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class GroupConversationEnvelope
{
    public GroupConversationEnvelope(
        GroupConversationCommand command,
        Func<GroupConversationResult, CancellationToken, ValueTask> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Command = command;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public GroupConversationCommand Command { get; }
    public Func<GroupConversationResult, CancellationToken, ValueTask> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

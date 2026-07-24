using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageReactionEnvelope
{
    public MessageReactionEnvelope(
        MessageReactionCommand command,
        Func<MessageReactionResult, CancellationToken, Task> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Command = command;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public MessageReactionCommand Command { get; }
    public Func<MessageReactionResult, CancellationToken, Task> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

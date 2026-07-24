using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditEnvelope
{
    public MessageEditEnvelope(
        MessageEditCommand command,
        Func<MessageEditResult, CancellationToken, Task> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Command = command;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public MessageEditCommand Command { get; }
    public Func<MessageEditResult, CancellationToken, Task> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

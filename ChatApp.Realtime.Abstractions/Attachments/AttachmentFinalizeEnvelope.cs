using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Attachments;

public sealed class AttachmentFinalizeEnvelope
{
    public AttachmentFinalizeEnvelope(
        AttachmentFinalizeCommand command,
        Func<AttachmentFinalizeResult, CancellationToken, ValueTask> replyAsync,
        ActivityContext parentContext = default,
        long? trustedUserId = null)
    {
        Command = command;
        ReplyAsync = replyAsync;
        ParentContext = parentContext;
        TrustedUserId = trustedUserId;
    }

    public AttachmentFinalizeCommand Command { get; }
    public Func<AttachmentFinalizeResult, CancellationToken, ValueTask> ReplyAsync { get; }
    public ActivityContext ParentContext { get; }
    public long? TrustedUserId { get; }
}

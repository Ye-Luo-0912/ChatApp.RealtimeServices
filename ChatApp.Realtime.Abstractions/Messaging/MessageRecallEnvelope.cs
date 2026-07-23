using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageRecallEnvelope(
    MessageRecallCommand command,
    Func<MessageRecallResult, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public MessageRecallCommand Command { get; } = command;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(MessageRecallResult result, CancellationToken ct = default) =>
        replyAsync(result, ct);
}

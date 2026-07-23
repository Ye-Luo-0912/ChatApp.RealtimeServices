using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationMarkReadEnvelope(
    ConversationMarkReadCommand command,
    Func<ConversationMarkReadResult, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public ConversationMarkReadCommand Command { get; } = command;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(ConversationMarkReadResult result, CancellationToken ct = default) =>
        replyAsync(result, ct);
}

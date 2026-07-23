using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Conversations;

public sealed class ConversationSetPrefsEnvelope(
    ConversationSetPrefsCommand command,
    Func<ConversationSetPrefsResult, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public ConversationSetPrefsCommand Command { get; } = command;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(ConversationSetPrefsResult result, CancellationToken ct = default) =>
        replyAsync(result, ct);
}

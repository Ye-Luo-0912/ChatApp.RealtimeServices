using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Sync;

public sealed class SyncBootstrapQueryEnvelope(
    SyncBootstrapQuery query,
    Func<SyncBootstrapPage, CancellationToken, ValueTask> replyAsync,
    ActivityContext parentContext = default,
    long? trustedUserId = null)
{
    public SyncBootstrapQuery Query { get; } = query;
    public ActivityContext ParentContext { get; } = parentContext;
    public long? TrustedUserId { get; } = trustedUserId;

    public ValueTask ReplyAsync(SyncBootstrapPage page, CancellationToken ct = default) =>
        replyAsync(page, ct);
}

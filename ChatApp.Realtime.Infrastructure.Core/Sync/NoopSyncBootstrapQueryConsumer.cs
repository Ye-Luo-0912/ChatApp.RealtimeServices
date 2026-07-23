using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Sync;

namespace ChatApp.Realtime.Infrastructure.Core.Sync;

public sealed class NoopSyncBootstrapQueryConsumer : ISyncBootstrapQueryConsumer
{
    public async IAsyncEnumerable<SyncBootstrapQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

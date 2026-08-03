using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

public sealed class NoopRelationshipListQueryConsumer : IRelationshipListQueryConsumer
{
    public async IAsyncEnumerable<RelationshipListQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
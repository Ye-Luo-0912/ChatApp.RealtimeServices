using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class NoopGroupConversationConsumer : IGroupConversationConsumer
{
    public async IAsyncEnumerable<GroupConversationEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
